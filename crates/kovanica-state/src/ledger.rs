//! Applying transactions to the UTXO set — the ledger's state-transition rules.
//!
//! Two layers live here:
//!
//! * [`apply_block`] — the strict, atomic state transition for one block's
//!   worth of transactions against a [`UtxoSet`]. It validates every spend
//!   (existence, no double-spend within the block, signature, value
//!   conservation) and the coinbase (issuance ≤ subsidy + fees), then commits.
//!   On *any* error the UTXO set is left untouched.
//! * [`apply_dag`] — the bridge to consensus. It walks
//!   `kovanica_dag::Dag::linearize()` (the deterministic GHOSTDAG total order),
//!   decodes each block's payload into transactions, and applies them in that
//!   order. This is what makes the ledger a *DAG* ledger: parallel blocks are
//!   ordered by GHOSTDAG, and a conflicting spend loses simply because the block
//!   that wins the linearization spent the output first.
//!
//! ## Rules, precisely
//!
//! For a non-coinbase transaction against the current UTXO set:
//! 1. it has at least one input and one output;
//! 2. no outpoint is spent twice within the same transaction;
//! 3. every spent outpoint is currently unspent (exists in the set);
//! 4. every input's signature verifies against the spent output's owner over the
//!    transaction's [`sighash`](crate::tx::Transaction::sighash);
//! 5. every output value is non-zero;
//! 6. outputs do not exceed inputs; the difference is the fee.
//!
//! A block may begin with a single coinbase (input-less) transaction. Regular
//! transactions are applied first, in list order, accumulating fees; then the
//! coinbase's outputs are validated to sum to **at most** `subsidy + fees` and
//! applied last. Because the coinbase is applied last, its outputs are not
//! spendable within the same block (a light-touch maturity rule).
//!
//! ## Deliberate first-slice simplifications
//!
//! * `subsidy` is a single per-block constant passed in, not a halving schedule.
//! * No coinbase maturity beyond "not in the same block"; no fee floor; no tx
//!   size/weight limits.
//! * [`apply_dag`] applies against a fresh state each call (the batch view). The
//!   incremental [`Ledger`] follows the selected tip, so re-orgs above the
//!   finality point are implicit (no revert), and [`Ledger::with_finality`] prunes
//!   the stored state of final blocks and rejects blocks built on final history.
//!   Pruning is of the per-block *state* only; the DAG itself is still append-only.

use std::collections::{HashMap, HashSet};

use kovanica_dag::{
    decode_snapshot, Block, BlockId, Dag, DagError, KParam, Retarget, SnapshotError,
};

use crate::keys::verify;

/// Halving schedule for block subsidy.
///
/// The subsidy starts at `genesis_subsidy` and halves every `halving_era` blocks
/// along the selected-parent chain. Genesis (height 0) gets the full subsidy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HalvingSchedule {
    /// Subsidy at genesis (height 0).
    pub genesis_subsidy: u64,
    /// Number of blocks per halving era.
    pub halving_era: u64,
}

impl HalvingSchedule {
    /// Create a new halving schedule.
    pub const fn new(genesis_subsidy: u64, halving_era: u64) -> Self {
        Self {
            genesis_subsidy,
            halving_era,
        }
    }

    /// Compute the subsidy for a block at `height` (height 0 = genesis).
    pub fn subsidy_at(&self, height: u64) -> u64 {
        let era = height / self.halving_era;
        if era >= 63 {
            0
        } else {
            self.genesis_subsidy >> era
        }
    }
}

/// Default halving era: 1000 blocks.
pub const DEFAULT_HALVING_ERA: u64 = 1_000;
use crate::tx::{
    decode_block_payload, encode_block_payload, DecodeError, OutPoint, Transaction, TxId,
};
use crate::utxo::UtxoSet;
use crate::validation::TxStructureValidator;

/// Why a transaction or block could not be applied.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LedgerError {
    /// A spent outpoint is not in the UTXO set (missing, already spent, or —
    /// for a same-block coinbase output — not yet mature).
    MissingInput(OutPoint),
    /// The same outpoint is spent twice within one transaction.
    DuplicateInput(OutPoint),
    /// A created output's outpoint already exists (id collision / replay).
    OutputAlreadyExists(OutPoint),
    /// An input's signature did not verify against the spent output's owner.
    BadSignature { tx: TxId, input: usize },
    /// A transaction's outputs exceed its inputs (it would mint value).
    ValueNotConserved { tx: TxId, inputs: u64, outputs: u64 },
    /// A value sum overflowed `u64`.
    ValueOverflow,
    /// A non-coinbase transaction had no inputs, or a coinbase had no outputs to
    /// carry any value — i.e. a structurally empty transaction where the model
    /// requires content.
    EmptyTransaction(TxId),
    /// An output has zero value.
    ZeroValueOutput(TxId),
    /// A coinbase (input-less) transaction appeared somewhere other than first.
    MisplacedCoinbase(TxId),
    /// The coinbase claims more than `subsidy + fees` allows.
    CoinbaseOverspend { claimed: u64, allowed: u64 },
    /// A block's payload could not be decoded into transactions.
    Payload(DecodeError),
}

impl core::fmt::Display for LedgerError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            LedgerError::MissingInput(op) => write!(f, "missing/spent input {op:?}"),
            LedgerError::DuplicateInput(op) => write!(f, "duplicate input {op:?}"),
            LedgerError::OutputAlreadyExists(op) => write!(f, "output already exists {op:?}"),
            LedgerError::BadSignature { tx, input } => {
                write!(f, "bad signature on input {input} of {tx}")
            }
            LedgerError::ValueNotConserved {
                tx,
                inputs,
                outputs,
            } => write!(
                f,
                "value not conserved in {tx}: in {inputs} < out {outputs}"
            ),
            LedgerError::ValueOverflow => f.write_str("value sum overflowed"),
            LedgerError::EmptyTransaction(tx) => write!(f, "empty transaction {tx}"),
            LedgerError::ZeroValueOutput(tx) => write!(f, "zero-value output in {tx}"),
            LedgerError::MisplacedCoinbase(tx) => write!(f, "misplaced coinbase {tx}"),
            LedgerError::CoinbaseOverspend { claimed, allowed } => {
                write!(
                    f,
                    "coinbase overspend: claimed {claimed} > allowed {allowed}"
                )
            }
            LedgerError::Payload(e) => write!(f, "payload decode: {e}"),
        }
    }
}

impl std::error::Error for LedgerError {}

/// What a successfully applied block moved: total fees collected from its
/// transactions and total value minted by its coinbase.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct BlockSummary {
    /// Sum of `inputs − outputs` across the block's non-coinbase transactions.
    pub fees: u64,
    /// Total value claimed by the block's coinbase (0 if none).
    pub minted: u64,
}

/// Apply one block's transactions to `utxo`, atomically.
///
/// `txs` is the block's transaction list; if the first has no inputs it is the
/// coinbase. `subsidy` is the issuance allowance for this block. Returns a
/// [`BlockSummary`] on success. On any error, `utxo` is left exactly as it was.
pub fn apply_block(
    utxo: &mut UtxoSet,
    txs: &[Transaction],
    subsidy: u64,
) -> Result<BlockSummary, LedgerError> {
    // Stage all changes on a copy; only commit if the whole block validates, so
    // a rejected block has no effect (atomicity).
    let mut staging = utxo.clone();

    let mut coinbase: Option<&Transaction> = None;
    let mut total_fees: u64 = 0;

    for (i, tx) in txs.iter().enumerate() {
        if tx.is_coinbase() {
            if i != 0 {
                return Err(LedgerError::MisplacedCoinbase(tx.id()));
            }
            coinbase = Some(tx);
            continue; // applied last, after fees are known
        }
        let fee = apply_regular(&mut staging, tx)?;
        total_fees = total_fees
            .checked_add(fee)
            .ok_or(LedgerError::ValueOverflow)?;
    }

    let allowed = subsidy
        .checked_add(total_fees)
        .ok_or(LedgerError::ValueOverflow)?;
    let minted = match coinbase {
        Some(cb) => apply_coinbase(&mut staging, cb, allowed)?,
        None => 0,
    };

    *utxo = staging;
    Ok(BlockSummary {
        fees: total_fees,
        minted,
    })
}

/// Validate and apply a regular (non-coinbase) transaction, returning its fee.
fn apply_regular(staging: &mut UtxoSet, tx: &Transaction) -> Result<u64, LedgerError> {
    if tx.inputs().is_empty() || tx.outputs().is_empty() {
        return Err(LedgerError::EmptyTransaction(tx.id()));
    }

    let sighash = tx.sighash();
    let mut seen: HashSet<OutPoint> = HashSet::with_capacity(tx.inputs().len());
    let mut sum_in: u64 = 0;
    for (i, input) in tx.inputs().iter().enumerate() {
        if !seen.insert(input.outpoint) {
            return Err(LedgerError::DuplicateInput(input.outpoint));
        }
        let prev = staging
            .get(&input.outpoint)
            .ok_or(LedgerError::MissingInput(input.outpoint))?;
        if !verify(&prev.owner, &sighash, &input.signature.to_bytes()) {
            return Err(LedgerError::BadSignature {
                tx: tx.id(),
                input: i,
            });
        }
        sum_in = sum_in
            .checked_add(prev.value)
            .ok_or(LedgerError::ValueOverflow)?;
    }

    let mut sum_out: u64 = 0;
    for output in tx.outputs() {
        if output.value == 0 {
            return Err(LedgerError::ZeroValueOutput(tx.id()));
        }
        sum_out = sum_out
            .checked_add(output.value)
            .ok_or(LedgerError::ValueOverflow)?;
    }

    if sum_out > sum_in {
        return Err(LedgerError::ValueNotConserved {
            tx: tx.id(),
            inputs: sum_in,
            outputs: sum_out,
        });
    }

    // Validation passed; mutate the staging set. (Any error above returned
    // before this point, so partial mutation cannot leak — and `apply_block`
    // discards `staging` unless the whole block succeeds.)
    let txid = tx.id();
    for input in tx.inputs() {
        staging.remove(&input.outpoint);
    }
    add_outputs(staging, txid, tx)?;

    Ok(sum_in - sum_out)
}

/// Validate and apply a coinbase transaction, returning the value minted.
fn apply_coinbase(
    staging: &mut UtxoSet,
    cb: &Transaction,
    allowed: u64,
) -> Result<u64, LedgerError> {
    let mut claimed: u64 = 0;
    for output in cb.outputs() {
        if output.value == 0 {
            return Err(LedgerError::ZeroValueOutput(cb.id()));
        }
        claimed = claimed
            .checked_add(output.value)
            .ok_or(LedgerError::ValueOverflow)?;
    }
    if claimed > allowed {
        return Err(LedgerError::CoinbaseOverspend { claimed, allowed });
    }
    add_outputs(staging, cb.id(), cb)?;
    Ok(claimed)
}

/// Insert every output of `tx` into `staging`, keyed by `(txid, index)`,
/// rejecting any outpoint that already exists.
fn add_outputs(staging: &mut UtxoSet, txid: TxId, tx: &Transaction) -> Result<(), LedgerError> {
    for (i, output) in tx.outputs().iter().enumerate() {
        let outpoint = OutPoint::new(txid, i as u32);
        if staging.insert(outpoint, *output).is_some() {
            return Err(LedgerError::OutputAlreadyExists(outpoint));
        }
    }
    Ok(())
}

/// The result of applying a whole DAG's worth of blocks in linearized order.
#[derive(Clone, Debug, Default)]
pub struct LedgerRun {
    /// The final UTXO set after applying every accepted block.
    pub utxo: UtxoSet,
    /// Blocks that applied cleanly, in linearization order.
    pub accepted: Vec<BlockId>,
    /// Blocks rejected as invalid, each with the reason. A block is rejected —
    /// not fatal — so a conflicting/invalid block never halts the ledger; it
    /// simply has no effect. Determined entirely by the GHOSTDAG order.
    pub rejected: Vec<(BlockId, LedgerError)>,
}

/// Apply an entire DAG: linearize it with GHOSTDAG, then apply each block's
/// transactions in that order against a fresh UTXO set.
///
/// This is a pure function of the DAG and `subsidy`: the linearization is
/// deterministic, and so is every state transition, so two nodes holding the
/// same DAG derive the identical [`LedgerRun`]. Conflicting spends across
/// parallel blocks are resolved by the linearization — the earlier block spends
/// the output; the later one is rejected with [`LedgerError::MissingInput`].
pub fn apply_dag(dag: &Dag, subsidy: u64) -> LedgerRun {
    let mut run = LedgerRun::default();
    for id in dag.linearize() {
        let payload = dag
            .block(&id)
            .expect("linearized id is present in the DAG")
            .payload();
        match decode_block_payload(payload) {
            Ok(txs) => match apply_block(&mut run.utxo, &txs, subsidy) {
                Ok(_) => run.accepted.push(id),
                Err(e) => run.rejected.push((id, e)),
            },
            Err(e) => run.rejected.push((id, LedgerError::Payload(e))),
        }
    }
    run
}

/// Why [`Ledger::insert`] could not add a block.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LedgerInsertError {
    /// The DAG rejected the block on structure (missing/duplicate parents, or an
    /// installed structural validator).
    Dag(DagError),
    /// The block's transactions are invalid against its view's UTXO state
    /// (a stateful rule: a missing/already-spent input, a bad signature, value
    /// not conserved, or coinbase overspend).
    State(LedgerError),
    /// The block builds on final history — its selected parent's blue score is
    /// below the finality point, so it is rejected (a deep re-org).
    Finality {
        parent_score: u64,
        finality_score: u64,
    },
}

impl From<DagError> for LedgerInsertError {
    fn from(e: DagError) -> Self {
        LedgerInsertError::Dag(e)
    }
}

impl From<LedgerError> for LedgerInsertError {
    fn from(e: LedgerError) -> Self {
        LedgerInsertError::State(e)
    }
}

impl core::fmt::Display for LedgerInsertError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            LedgerInsertError::Dag(e) => write!(f, "dag rejected block: {e}"),
            LedgerInsertError::State(e) => write!(f, "invalid block state: {e}"),
            LedgerInsertError::Finality {
                parent_score,
                finality_score,
            } => write!(
                f,
                "finality violation: selected parent blue score {parent_score} < finality {finality_score}"
            ),
        }
    }
}

impl std::error::Error for LedgerInsertError {}

/// A DAG together with the **per-block UTXO state** each block induces.
///
/// [`apply_dag`] is the batch view: it (re)linearizes a finished DAG and folds
/// every transaction from scratch. A `Ledger` is the *incremental* view. It owns
/// a [`Dag`] and, for every block `B`, stores the UTXO set of `B`'s own view —
/// the state after applying, in the recursive GHOSTDAG order, every transaction
/// in `past(B) ∪ {B}`. That state is built cheaply from `B`'s selected parent:
///
/// ```text
/// state(B) = apply( state(selected_parent(B)),
///                   mergeset(B) blocks' transactions (in order),
///                   B's own transactions )
/// ```
///
/// Because `B`'s transactions are checked against this pre-state *before* the
/// block is committed (via [`Dag::preview`]), a block that is invalid in its own
/// view — it double-spends an ancestor's output, carries a bad signature, mints
/// value, or overspends its coinbase — is rejected at insert and never enters
/// the DAG. (Two *parallel* blocks that spend the same output are each valid in
/// their own view and both admitted; their conflict is resolved only in the view
/// of a block that merges them, exactly as GHOSTDAG intends.)
///
/// The structural [`TxStructureValidator`] is also installed on the underlying
/// DAG, so malformed blocks are rejected even if the DAG is used directly.
///
/// State is kept per block in full (mirroring the crate's other O(n²) first-slice
/// simplifications); storing compact per-block diffs is a later optimisation.
///
/// ## Finality and pruning
///
/// A `Ledger` built with [`Ledger::with_finality`] treats blocks more than
/// `finality_depth` blue score below the selected tip as **final**: no new block
/// may build on them (their selected parent being final is a
/// [`LedgerInsertError::Finality`]), so their stored per-block state is no longer
/// needed and is **pruned**, bounding memory. This also makes the selected chain
/// stable below the finality point — a deep re-org is rejected rather than
/// applied. Above the finality point, re-orgs are implicit: [`Ledger::ledger_state`]
/// always follows the current selected tip, so a heavier branch takes over with no
/// explicit revert. [`Ledger::new`] uses an unbounded depth — it never prunes and
/// never rejects on finality.
pub struct Ledger {
    dag: Dag,
    schedule: HalvingSchedule,
    genesis: BlockId,
    /// Blocks this far in blue score below the selected tip are final; their
    /// state is pruned and they cannot be built on. `u64::MAX` = never.
    finality_depth: u64,
    /// Per-block view UTXO state: `states[&b]` is the ledger state in `b`'s view.
    /// Final blocks (below the finality point) are pruned from this map.
    states: HashMap<BlockId, UtxoSet>,
    /// Block heights: `heights[&b]` is the height of block `b` in the selected chain.
    heights: HashMap<BlockId, u64>,
}

impl Ledger {
    /// Create a ledger whose genesis block carries `genesis_txs` (typically a
    /// single coinbase minting the initial supply). `k` is the GHOSTDAG
    /// parameter and `schedule` the halving schedule for per-block issuance.
    ///
    /// Fails if `genesis_txs` are not a valid block on an empty UTXO set.
    pub fn new(
        k: KParam,
        schedule: HalvingSchedule,
        genesis_txs: &[Transaction],
    ) -> Result<Self, LedgerError> {
        let genesis_subsidy = schedule.subsidy_at(0);
        let mut state = UtxoSet::new();
        apply_block(&mut state, genesis_txs, genesis_subsidy)?;

        let genesis = Block::genesis(1, 0, 0, encode_block_payload(genesis_txs));
        let genesis_id = genesis.id();
        let dag = Dag::with_validator(k, genesis, Box::new(TxStructureValidator));

        let mut states = HashMap::new();
        states.insert(genesis_id, state);
        let mut heights = HashMap::new();
        heights.insert(genesis_id, 0);
        Ok(Self {
            dag,
            schedule,
            genesis: genesis_id,
            finality_depth: u64::MAX,
            states,
            heights,
        })
    }

    /// Like [`Ledger::new`], but with a finite finality depth: blocks more than
    /// `finality_depth` blue score below the selected tip become final — they may
    /// not be built on, and their per-block state is pruned. See the type docs.
    pub fn with_finality(
        k: KParam,
        schedule: HalvingSchedule,
        genesis_txs: &[Transaction],
        finality_depth: u64,
    ) -> Result<Self, LedgerError> {
        let mut ledger = Self::new(k, schedule, genesis_txs)?;
        ledger.finality_depth = finality_depth;
        Ok(ledger)
    }

    /// Borrow the underlying DAG (for consensus queries: tips, ghostdag,
    /// `linearize`, `selected_chain`, …).
    pub fn dag(&self) -> &Dag {
        &self.dag
    }

    /// Enable consensus-enforced difficulty on the underlying DAG with policy
    /// `retarget`: every subsequent [`Ledger::insert`] additionally requires the
    /// block's `work` to equal [`Dag::next_work_target`] and its timestamp not to
    /// precede any parent's. See [`Dag::set_difficulty`].
    pub fn set_difficulty(&mut self, retarget: Retarget) {
        self.dag.set_difficulty(retarget);
    }

    /// Enable (or disable) consensus-enforced proof-of-work on the underlying
    /// DAG: every subsequent [`Ledger::insert`] additionally requires the block's
    /// id to meet its `work` target — i.e. the block must have been mined (its
    /// `nonce` chosen so the hash is small enough). See
    /// [`Dag::set_proof_of_work`] and [`kovanica_dag::pow`]. Off by default.
    pub fn set_proof_of_work(&mut self, enabled: bool) {
        self.dag.set_proof_of_work(enabled);
    }

    /// The finality depth (blue score below the selected tip). `u64::MAX` means
    /// finality/pruning is disabled.
    pub fn finality_depth(&self) -> u64 {
        self.finality_depth
    }

    /// The blue-score threshold below which blocks are final: blocks with a blue
    /// score `< finality_score()` are pruned and may not be built on. `0` when
    /// finality is disabled or the DAG is not yet `finality_depth` deep.
    pub fn finality_score(&self) -> u64 {
        if self.finality_depth == u64::MAX {
            return 0;
        }
        let tip = self.dag.selected_tip();
        let max = self.dag.ghostdag(&tip).map_or(0, |g| g.blue_score);
        max.saturating_sub(self.finality_depth)
    }

    /// The genesis block id.
    pub fn genesis(&self) -> BlockId {
        self.genesis
    }

    /// The subsidy for the next block (based on the selected tip's height).
    pub fn subsidy(&self) -> u64 {
        let tip = self.dag.selected_tip();
        let height = self.heights.get(&tip).copied().unwrap_or(0);
        self.schedule.subsidy_at(height + 1)
    }

    /// The halving schedule.
    pub fn schedule(&self) -> HalvingSchedule {
        self.schedule
    }

    /// The UTXO state in `block`'s own view, if `block` is present.
    pub fn state(&self, block: &BlockId) -> Option<&UtxoSet> {
        self.states.get(block)
    }

    /// Insert a block referencing `parents`, carrying `work`, `timestamp_ms`,
    /// `nonce`, and `txs`.
    ///
    /// Validates `txs` against the block's view UTXO state and, on success, adds
    /// the block to the DAG and stores its per-block state. On any error the
    /// ledger and DAG are left unchanged and the block is not added. When
    /// proof-of-work is enforced (see [`Ledger::set_proof_of_work`]), `nonce`
    /// must have been chosen so the block's id meets its `work` target — i.e. the
    /// caller mined the block (see [`kovanica_dag::pow::mine`]); with PoW off,
    /// `nonce` is unconstrained (pass `0`).
    pub fn insert(
        &mut self,
        parents: Vec<BlockId>,
        work: u128,
        timestamp_ms: u64,
        nonce: u64,
        txs: &[Transaction],
    ) -> Result<BlockId, LedgerInsertError> {
        let block = Block::new(
            parents,
            work,
            timestamp_ms,
            nonce,
            encode_block_payload(txs),
        );

        // Build the block's view pre-state: its selected parent's state with the
        // mergeset blocks' transactions applied in order. Previewing gets the
        // selected parent and mergeset without mutating the DAG.
        let preview = self.dag.preview(&block)?;

        // Finality: a block may not build on final history. Its selected parent
        // being final means its state has been pruned, so this check also
        // guarantees the state lookup below succeeds.
        let parent_score = self
            .dag
            .ghostdag(&preview.selected_parent)
            .map_or(0, |g| g.blue_score);
        let finality_score = self.finality_score();
        if parent_score < finality_score {
            return Err(LedgerInsertError::Finality {
                parent_score,
                finality_score,
            });
        }

        let parent_height = self
            .heights
            .get(&preview.selected_parent)
            .copied()
            .unwrap_or(0);
        let new_height = parent_height + 1;

        let mut state = self
            .states
            .get(&preview.selected_parent)
            .expect("non-final selected parent always has a stored state")
            .clone();
        for merged in &preview.mergeset {
            let merged_height = self.heights.get(merged).copied().unwrap_or(0);
            let payload = self
                .dag
                .block(merged)
                .expect("mergeset block is in the DAG")
                .payload();
            if let Ok(merged_txs) = decode_block_payload(payload) {
                // A merged block that conflicts in this view simply does not
                // apply — its transactions were valid in their own view, not
                // necessarily here. This mirrors apply_dag's per-block reject.
                let _ = apply_block(
                    &mut state,
                    &merged_txs,
                    self.schedule.subsidy_at(merged_height),
                );
            }
        }

        // Stateful validation: the block's own transactions must be valid against
        // its view pre-state. Failure rejects the block before it enters the DAG.
        apply_block(&mut state, txs, self.schedule.subsidy_at(new_height))?;

        // Commit: add to the DAG (structural checks run here) and store the state.
        let id = self.dag.insert(block)?;
        self.states.insert(id, state);
        self.heights.insert(id, new_height);
        self.prune();
        Ok(id)
    }

    /// Drop the stored state of every block that is now final (below
    /// [`Ledger::finality_score`]). Finality only rises, so a pruned block stays
    /// prunable; and only final blocks are dropped, which are never a future
    /// block's selected parent (that is a finality violation) nor needed by
    /// [`Ledger::ledger_state`] (which starts from the selected tip).
    fn prune(&mut self) {
        let threshold = self.finality_score();
        if threshold == 0 {
            return;
        }
        let stale: Vec<BlockId> = self
            .states
            .keys()
            .copied()
            .filter(|id| {
                self.dag
                    .ghostdag(id)
                    .is_some_and(|g| g.blue_score < threshold)
            })
            .collect();
        for id in stale {
            self.states.remove(&id);
            self.heights.remove(&id);
        }
    }

    /// The full current ledger state: every block applied in linearized order.
    ///
    /// Built incrementally as the selected tip's view state plus the side blocks
    /// under the other tips (the linearization's tail).
    pub fn ledger_state(&self) -> UtxoSet {
        let order = self.dag.linearize();
        let selected_tip = self.dag.selected_tip();
        let tip_pos = order
            .iter()
            .position(|b| *b == selected_tip)
            .expect("selected tip is in the order");

        let mut state = self
            .states
            .get(&selected_tip)
            .expect("selected tip has a stored state")
            .clone();
        for block in &order[tip_pos + 1..] {
            let height = self.heights.get(block).copied().unwrap_or(0);
            let payload = self
                .dag
                .block(block)
                .expect("block is in the DAG")
                .payload();
            if let Ok(txs) = decode_block_payload(payload) {
                let _ = apply_block(&mut state, &txs, self.schedule.subsidy_at(height));
            }
        }
        state
    }

    /// Serialise the ledger to a self-contained snapshot: the halving schedule plus the
    /// underlying DAG's replay log (see [`Dag::write_snapshot`]). Per-block UTXO
    /// state is *not* stored — it is recomputed on load by replaying blocks
    /// through [`Ledger::insert`], so nothing derived is trusted from disk.
    pub fn write_snapshot(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&LEDGER_MAGIC);
        buf.extend_from_slice(&LEDGER_VERSION.to_le_bytes());
        buf.extend_from_slice(&self.schedule.genesis_subsidy.to_le_bytes());
        buf.extend_from_slice(&self.schedule.halving_era.to_le_bytes());
        buf.extend_from_slice(&self.dag.write_snapshot());
        buf
    }

    /// Rebuild a ledger from a snapshot by replaying its blocks. The full state
    /// (and each block's view state) is recomputed, so the restored ledger is
    /// identical to the original — except that it restores at **unbounded
    /// finality** (the snapshot stores blocks, not the runtime finality policy);
    /// re-apply [`Ledger::with_finality`]'s depth after loading if wanted.
    pub fn read_snapshot(bytes: &[u8]) -> Result<Ledger, LedgerSnapshotError> {
        if bytes.len() < 4 || bytes[..4] != LEDGER_MAGIC {
            return Err(LedgerSnapshotError::BadMagic);
        }
        if bytes.len() < 22 {
            return Err(LedgerSnapshotError::Dag(SnapshotError::UnexpectedEof));
        }
        let version = u16::from_le_bytes([bytes[4], bytes[5]]);
        if version != LEDGER_VERSION {
            return Err(LedgerSnapshotError::UnsupportedVersion(version));
        }
        let genesis_subsidy =
            u64::from_le_bytes(bytes[6..14].try_into().expect("14 - 6 == 8 bytes"));
        let halving_era = u64::from_le_bytes(bytes[14..22].try_into().expect("22 - 14 == 8 bytes"));
        let schedule = HalvingSchedule::new(genesis_subsidy, halving_era);

        let snapshot = decode_snapshot(&bytes[22..]).map_err(LedgerSnapshotError::Dag)?;
        let mut blocks = snapshot.blocks.into_iter();
        let genesis = blocks.next().ok_or(LedgerSnapshotError::Empty)?;
        let genesis_txs =
            decode_block_payload(genesis.payload()).map_err(LedgerSnapshotError::Payload)?;
        let mut ledger = Ledger::new(snapshot.k, schedule, &genesis_txs)
            .map_err(LedgerSnapshotError::Genesis)?;
        for block in blocks {
            let txs =
                decode_block_payload(block.payload()).map_err(LedgerSnapshotError::Payload)?;
            ledger
                .insert(
                    block.parents().to_vec(),
                    block.work(),
                    block.timestamp_ms(),
                    block.nonce(),
                    &txs,
                )
                .map_err(LedgerSnapshotError::Rebuild)?;
        }
        Ok(ledger)
    }
}

/// Magic prefix identifying a Kovanica ledger snapshot (`"KVLG"`).
const LEDGER_MAGIC: [u8; 4] = *b"KVLG";
/// Ledger snapshot format version.
const LEDGER_VERSION: u16 = 1;

/// Why a ledger snapshot could not be decoded or replayed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LedgerSnapshotError {
    /// The bytes did not start with the expected magic prefix.
    BadMagic,
    /// The snapshot version is not supported by this build.
    UnsupportedVersion(u16),
    /// The embedded DAG snapshot could not be decoded.
    Dag(SnapshotError),
    /// A block's payload was not valid transaction encoding.
    Payload(DecodeError),
    /// Applying the genesis transactions failed.
    Genesis(LedgerError),
    /// Replaying a block through `insert` failed.
    Rebuild(LedgerInsertError),
    /// The snapshot contained no genesis block.
    Empty,
}

impl core::fmt::Display for LedgerSnapshotError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            LedgerSnapshotError::BadMagic => f.write_str("not a kovanica ledger snapshot"),
            LedgerSnapshotError::UnsupportedVersion(v) => {
                write!(f, "unsupported ledger snapshot version {v}")
            }
            LedgerSnapshotError::Dag(e) => write!(f, "dag snapshot: {e}"),
            LedgerSnapshotError::Payload(e) => write!(f, "payload decode: {e}"),
            LedgerSnapshotError::Genesis(e) => write!(f, "genesis: {e}"),
            LedgerSnapshotError::Rebuild(e) => write!(f, "replaying block: {e}"),
            LedgerSnapshotError::Empty => f.write_str("snapshot has no genesis block"),
        }
    }
}

impl std::error::Error for LedgerSnapshotError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keys::KeyPair;
    use crate::tx::{TxId, TxOutput};

    // A funded outpoint owned by `kp`, seeded directly into a UTXO set so unit
    // tests need no coinbase plumbing.
    fn funded(set: &mut UtxoSet, kp: &KeyPair, value: u64, seed: u8) -> OutPoint {
        let op = OutPoint::new(TxId::from_bytes([seed; 32]), 0);
        set.insert(op, TxOutput::new(value, kp.address()));
        op
    }

    #[test]
    fn transfer_conserves_value_and_pays_fee() {
        let alice = KeyPair::from_u64(1);
        let bob = KeyPair::from_u64(2);
        let mut utxo = UtxoSet::new();
        let op = funded(&mut utxo, &alice, 100, 1);

        let tx = Transaction::signed(
            &[(op, &alice)],
            vec![TxOutput::new(90, bob.address())],
            vec![],
        );
        let summary = apply_block(&mut utxo, &[tx], 0).unwrap();

        assert_eq!(summary.fees, 10); // 100 in − 90 out
        assert_eq!(utxo.balance(&bob.address()), 90);
        assert_eq!(utxo.balance(&alice.address()), 0);
        assert!(!utxo.contains(&op), "spent input is gone");
    }

    #[test]
    fn bad_signature_is_rejected_and_atomic() {
        let alice = KeyPair::from_u64(1);
        let mallory = KeyPair::from_u64(9);
        let bob = KeyPair::from_u64(2);
        let mut utxo = UtxoSet::new();
        let op = funded(&mut utxo, &alice, 100, 1);
        let before = utxo.total_value();

        // Mallory signs a spend of Alice's output.
        let tx = Transaction::signed(
            &[(op, &mallory)],
            vec![TxOutput::new(50, bob.address())],
            vec![],
        );
        let err = apply_block(&mut utxo, &[tx], 0).unwrap_err();

        assert!(matches!(err, LedgerError::BadSignature { input: 0, .. }));
        assert_eq!(
            utxo.total_value(),
            before,
            "rejected block left state untouched"
        );
        assert!(utxo.contains(&op));
    }

    #[test]
    fn overspend_is_rejected() {
        let alice = KeyPair::from_u64(1);
        let bob = KeyPair::from_u64(2);
        let mut utxo = UtxoSet::new();
        let op = funded(&mut utxo, &alice, 100, 1);

        let tx = Transaction::signed(
            &[(op, &alice)],
            vec![TxOutput::new(101, bob.address())],
            vec![],
        );
        let err = apply_block(&mut utxo, &[tx], 0).unwrap_err();
        assert!(matches!(
            err,
            LedgerError::ValueNotConserved {
                inputs: 100,
                outputs: 101,
                ..
            }
        ));
    }

    #[test]
    fn double_spend_within_block_is_rejected() {
        let alice = KeyPair::from_u64(1);
        let bob = KeyPair::from_u64(2);
        let mut utxo = UtxoSet::new();
        let op = funded(&mut utxo, &alice, 100, 1);

        let first = Transaction::signed(
            &[(op, &alice)],
            vec![TxOutput::new(90, bob.address())],
            b"1".to_vec(),
        );
        // Second tx spends the same outpoint; by application time it's gone.
        let second = Transaction::signed(
            &[(op, &alice)],
            vec![TxOutput::new(80, bob.address())],
            b"2".to_vec(),
        );
        let err = apply_block(&mut utxo, &[first, second], 0).unwrap_err();
        assert_eq!(err, LedgerError::MissingInput(op));
        // Atomic: neither spend took effect.
        assert!(utxo.contains(&op));
    }

    #[test]
    fn coinbase_may_claim_subsidy_plus_fees_but_no_more() {
        let alice = KeyPair::from_u64(1);
        let bob = KeyPair::from_u64(2);
        let miner = KeyPair::from_u64(3);
        let mut utxo = UtxoSet::new();
        let op = funded(&mut utxo, &alice, 100, 1);

        // Transfer leaves a fee of 10; subsidy 50 ⇒ coinbase may claim 60.
        let transfer = Transaction::signed(
            &[(op, &alice)],
            vec![TxOutput::new(90, bob.address())],
            vec![],
        );
        let good_cb =
            Transaction::coinbase(vec![TxOutput::new(60, miner.address())], b"h1".to_vec());
        let summary = apply_block(&mut utxo, &[good_cb, transfer.clone()], 50).unwrap();
        assert_eq!(
            summary,
            BlockSummary {
                fees: 10,
                minted: 60
            }
        );

        // Claiming 61 overspends.
        let mut utxo2 = UtxoSet::new();
        let op2 = funded(&mut utxo2, &alice, 100, 1);
        let transfer2 = Transaction::signed(
            &[(op2, &alice)],
            vec![TxOutput::new(90, bob.address())],
            vec![],
        );
        let greedy_cb =
            Transaction::coinbase(vec![TxOutput::new(61, miner.address())], b"h1".to_vec());
        let err = apply_block(&mut utxo2, &[greedy_cb, transfer2], 50).unwrap_err();
        assert_eq!(
            err,
            LedgerError::CoinbaseOverspend {
                claimed: 61,
                allowed: 60
            }
        );
    }
}
