//! The node: an in-memory [`Ledger`] and [`Mempool`], plus the operations a node
//! offers — bring up a genesis, build/pack/submit spends, produce blocks, gossip
//! blocks with peers, query balances and tips, and save/load its state.
//!
//! For demonstration and testing, actors are identified by a small integer
//! *seed* — the node derives `KeyPair::from_u64(seed)` for them and signs on
//! their behalf (single-UTXO coin selection: it spends one existing output that
//! covers the amount and returns the change). A real node never holds spending
//! keys or does wallet work; that lives client-side. This keeps the binary a
//! runnable, self-contained demo of the whole stack.

use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

use kovanica_dag::{pow, Block, BlockId, Dag, DagError};
use kovanica_state::{
    apply_block, decode_block_payload, encode_block_payload, verify, Address, HalvingSchedule,
    KeyPair, Ledger, LedgerError, LedgerInsertError, LedgerStore, OutPoint, Sig, Transaction, TxId,
    TxOutput, UtxoSet, DEFAULT_HALVING_ERA,
};

use crate::mempool::Mempool;

/// How far ahead of the local wall clock a received block's timestamp may sit
/// before the node rejects it: two hours, in milliseconds. This is **node
/// policy**, not pure-DAG consensus — it depends on the local clock, so it lives
/// at the block-acceptance layer, not in [`kovanica_dag`]. (Bitcoin uses the
/// same two-hour future-time bound.)
const MAX_FUTURE_DRIFT_MS: u64 = 2 * 60 * 60 * 1000; // 2 hours

/// The node's source of wall-clock time. Injectable so production timestamps and
/// the future-time bound are deterministic in tests (and controllable in
/// simulated environments) — see [`Node::set_now_ms`].
#[derive(Default)]
enum Clock {
    /// Real UNIX wall-clock time.
    #[default]
    Wall,
    /// A pinned time in milliseconds since the UNIX epoch.
    Fixed(u64),
}

/// Why a node operation failed.
#[derive(Debug)]
pub enum NodeError {
    /// An operation needed a ledger, but no genesis has been created yet.
    NotInitialized,
    /// `genesis` was called on an already-initialised node.
    AlreadyInitialized,
    /// A spend of zero value was requested.
    ZeroAmount,
    /// No single unspent output owned by the sender covers the amount (this node
    /// does not combine multiple outputs).
    InsufficientFunds,
    /// A coinbase transaction was submitted where a spend was expected.
    UnexpectedCoinbase,
    /// The supplied spend signature did not verify.
    BadSignature,
    /// Building the genesis ledger failed.
    Ledger(LedgerError),
    /// Submitting the block failed (structure or stateful validation).
    Insert(LedgerInsertError),
    /// Reading or writing the snapshot file failed.
    Io(String),
    /// Decoding a snapshot failed.
    Snapshot(String),
    /// A received block's timestamp is further ahead of the local wall clock than
    /// [`MAX_FUTURE_DRIFT_MS`] allows (node policy, not pure-DAG consensus).
    TimestampTooFarInFuture {
        /// The block's timestamp, in milliseconds.
        timestamp_ms: u64,
        /// The local wall-clock time it was checked against, in milliseconds.
        now_ms: u64,
    },
}

impl core::fmt::Display for NodeError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            NodeError::NotInitialized => f.write_str("no ledger yet — run `genesis` first"),
            NodeError::AlreadyInitialized => f.write_str("already initialised"),
            NodeError::ZeroAmount => f.write_str("amount must be non-zero"),
            NodeError::InsufficientFunds => {
                f.write_str("no unspent outputs cover the amount plus fee")
            }
            NodeError::UnexpectedCoinbase => f.write_str("coinbase transactions are not accepted"),
            NodeError::BadSignature => f.write_str("bad spend signature"),
            NodeError::Ledger(e) => write!(f, "genesis invalid: {e}"),
            NodeError::Insert(e) => write!(f, "{e}"),
            NodeError::Io(e) => write!(f, "io error: {e}"),
            NodeError::Snapshot(e) => write!(f, "bad snapshot: {e}"),
            NodeError::TimestampTooFarInFuture { timestamp_ms, now_ms } => write!(
                f,
                "block timestamp {timestamp_ms}ms is more than {MAX_FUTURE_DRIFT_MS}ms ahead of local time {now_ms}ms"
            ),
        }
    }
}

impl std::error::Error for NodeError {}

/// The result of a successful [`Node::send`]: the block that carried the spend
/// and the transaction's id.
#[derive(Clone, Copy, Debug)]
pub struct Sent {
    /// Id of the block that was inserted.
    pub block: BlockId,
    /// Id of the transfer transaction.
    pub tx: TxId,
}

/// An unsigned transfer ready for a wallet to sign.
#[derive(Clone, Debug)]
pub struct Prepared {
    /// The unsigned transaction (zeroed signatures).
    pub tx: Transaction,
    /// BLAKE3 sighash the wallet must sign.
    pub sighash: [u8; 32],
    /// Selected funding outpoint.
    pub outpoint: OutPoint,
    /// Value of that outpoint.
    pub value: u64,
    /// Protocol fee burned-or-paid to the miner (atoms).
    pub fee: u64,
}

/// The wire form of a block for gossip: everything a peer needs to re-insert it.
#[derive(Clone, Debug)]
pub struct BlockRecord {
    /// The block's parents.
    pub parents: Vec<BlockId>,
    /// The block's work weight.
    pub work: u128,
    /// The block's timestamp, in milliseconds.
    pub timestamp_ms: u64,
    /// The block's proof-of-work nonce. Carried so a peer reconstructs the exact
    /// same id (and, under enforced PoW, the block still meets its target).
    pub nonce: u64,
    /// The block's transactions.
    pub txs: Vec<Transaction>,
}

/// A running node holding the ledger and mempool in memory.
#[derive(Default)]
pub struct Node {
    ledger: Option<Ledger>,
    mempool: Mempool,
    clock: Clock,
    /// Address that receives the per-block KVNC subsidy coinbase.
    miner: Option<Address>,
}

/// Blocks per subsidy-halving era. Issuance is `cap >> (height / HALVING_ERA)`.
pub const HALVING_ERA: u64 = 1_000;
/// Floor: `max(1, subsidy / 500_000)`. On the 50 KVNC testnet that is 0.0001 KVNC.
pub const MIN_FEE_DIVISOR: u64 = 500_000;

impl Node {
    /// A fresh node with no ledger yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// The node's current wall-clock time in milliseconds since the UNIX epoch.
    /// With the default [`Clock::Wall`] this reads the system clock (returning 0
    /// if it is somehow before the epoch — no panic); a pinned clock returns its
    /// fixed value.
    fn now_ms(&self) -> u64 {
        match self.clock {
            Clock::Wall => SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |d| d.as_millis() as u64),
            Clock::Fixed(n) => n,
        }
    }

    /// Pin the node's clock to a fixed time (milliseconds since the UNIX epoch),
    /// making both produced-block timestamps and the future-time bound
    /// deterministic. Primarily for tests and controlled/simulated environments;
    /// a real node runs on the default wall clock.
    pub fn set_now_ms(&mut self, now_ms: u64) {
        self.clock = Clock::Fixed(now_ms);
    }

    /// The timestamp to stamp on a new block built on `parents`: the node's
    /// wall-clock now, clamped up to stay strictly after the latest parent
    /// (genesis is at 0). The wall clock makes timestamps meaningful; the clamp
    /// keeps them monotone even if the clock is behind or a parent is ahead, so
    /// they still satisfy the difficulty layer's "not older than any parent" rule
    /// (see [`kovanica_dag::Dag::set_difficulty`]).
    fn next_timestamp(&self, dag: &Dag, parents: &[BlockId]) -> u64 {
        let floor = parents
            .iter()
            .filter_map(|p| dag.block(p).map(|b| b.timestamp_ms()))
            .max()
            .map_or(0, |latest| latest + 1);
        self.now_ms().max(floor)
    }

    /// The proof-of-work nonce to stamp on a new block built on `parents` with
    /// `work`, `timestamp_ms`, and transactions `txs`.
    ///
    /// When proof-of-work is enforced on the ledger's DAG, mine the block —
    /// Nakamoto-style hash-target search over the nonce
    /// ([`kovanica_dag::pow::mine`]) — so its id meets its `work` target and it
    /// passes insert; otherwise `0` (no mining). The template here must be
    /// byte-identical to the block [`Ledger::insert`] will build (same parents,
    /// work, timestamp, and payload encoding) so the winning nonce carries over
    /// to exactly the same id.
    fn mine_nonce(
        dag: &Dag,
        parents: &[BlockId],
        work: u128,
        timestamp_ms: u64,
        txs: &[Transaction],
    ) -> u64 {
        if !dag.proof_of_work_enabled() {
            return 0;
        }
        let template = Block::new(
            parents.to_vec(),
            work,
            timestamp_ms,
            0,
            encode_block_payload(txs),
        );
        pow::mine(&template).nonce()
    }

    /// Enable (or disable) consensus-enforced proof-of-work on the ledger. Once
    /// enabled, produced blocks are mined and received blocks must meet their
    /// target. See [`Ledger::set_proof_of_work`]. Errors if not initialised.
    pub fn set_proof_of_work(&mut self, enabled: bool) -> Result<(), NodeError> {
        self.ledger
            .as_mut()
            .ok_or(NodeError::NotInitialized)?
            .set_proof_of_work(enabled);
        Ok(())
    }

    /// Whether consensus-enforced proof-of-work is on.
    pub fn proof_of_work(&self) -> bool {
        self.ledger()
            .map(|l| l.dag().proof_of_work_enabled())
            .unwrap_or(false)
    }

    /// Whether a genesis has been created.
    pub fn is_initialized(&self) -> bool {
        self.ledger.is_some()
    }

    /// The address the node uses for actor `seed`.
    pub fn address(seed: u64) -> Address {
        KeyPair::from_u64(seed).address()
    }

    /// Bring up the ledger: a genesis block whose coinbase mints `amount` to
    /// actor `founder_seed`, with GHOSTDAG parameter `k` and per-block `subsidy`.
    /// Returns the genesis block id and the founder's address.
    pub fn genesis(
        &mut self,
        k: u16,
        subsidy: u64,
        amount: u64,
        founder_seed: u64,
    ) -> Result<(BlockId, Address), NodeError> {
        if self.ledger.is_some() {
            return Err(NodeError::AlreadyInitialized);
        }
        let founder = Self::address(founder_seed);
        let coinbase =
            Transaction::coinbase(vec![TxOutput::new(amount, founder)], b"genesis".to_vec());
        let schedule = HalvingSchedule::new(subsidy, DEFAULT_HALVING_ERA);
        let ledger = Ledger::new(k, schedule, &[coinbase]).map_err(NodeError::Ledger)?;
        let genesis = ledger.genesis();
        self.ledger = Some(ledger);
        self.miner = Some(founder);
        Ok((genesis, founder))
    }

    /// Who receives the native-token (KVNC) subsidy on produced blocks.
    pub fn set_miner(&mut self, miner: Address) {
        self.miner = Some(miner);
    }

    /// Current miner address, if set.
    pub fn miner(&self) -> Option<Address> {
        self.miner
    }

    /// Protocol minimum fee for the next transfer, in atoms.
    pub fn min_fee(&self) -> u64 {
        let cap = self.ledger().map(|l| l.subsidy()).unwrap_or(1);
        (cap / MIN_FEE_DIVISOR).max(1)
    }

    /// KVNC atoms minted on the *next* produced block (decaying from the
    /// genesis subsidy cap). Coinbase still cannot exceed `ledger.subsidy()`.
    pub fn issuance(&self) -> Result<u64, NodeError> {
        let ledger = self.ledger()?;
        Ok(ledger.subsidy())
    }

    /// Compute the subsidy at a given height (height 0 = genesis).
    /// `cap` is the genesis subsidy. Halving era is `HALVING_ERA` (1000 blocks).
    pub fn issuance_at(cap: u64, height: u64) -> u64 {
        let era = height / HALVING_ERA;
        if era >= 63 {
            0
        } else {
            cap >> era
        }
    }

    pub(crate) fn ledger(&self) -> Result<&Ledger, NodeError> {
        self.ledger.as_ref().ok_or(NodeError::NotInitialized)
    }

    /// Pending mempool transactions in assembly order.
    pub fn pending_txs(&self) -> Vec<Transaction> {
        self.mempool.ordered()
    }

    /// The spendable balance of `owner` in the current full ledger state.
    pub fn balance(&self, owner: &Address) -> Result<u128, NodeError> {
        Ok(self.ledger()?.ledger_state().balance(owner))
    }

    /// The current tips.
    pub fn tips(&self) -> Result<Vec<BlockId>, NodeError> {
        Ok(self.ledger()?.dag().tips())
    }

    /// The selected (heaviest) tip.
    pub fn selected_tip(&self) -> Result<BlockId, NodeError> {
        Ok(self.ledger()?.dag().selected_tip())
    }

    /// Number of blocks in the DAG (including genesis).
    pub fn block_count(&self) -> Result<usize, NodeError> {
        Ok(self.ledger()?.dag().len())
    }

    /// Number of pending transactions in the mempool.
    pub fn pending_count(&self) -> usize {
        self.mempool.len()
    }

    /// Build a signed transfer of `amount` from actor `from_seed` to actor
    /// `to_seed`, selecting one of the sender's outputs that covers it and
    /// returning the change. Does not touch the ledger or mempool.
    fn build_transfer(
        &self,
        from_seed: u64,
        amount: u64,
        to_seed: u64,
    ) -> Result<Transaction, NodeError> {
        self.build_transfer_to(from_seed, amount, Self::address(to_seed))
    }

    /// Build a signed transfer from a seed actor to an arbitrary address.
    fn build_transfer_to(
        &self,
        from_seed: u64,
        amount: u64,
        to_addr: Address,
    ) -> Result<Transaction, NodeError> {
        if amount == 0 {
            return Err(NodeError::ZeroAmount);
        }
        let from = KeyPair::from_u64(from_seed);
        let unsigned = self.prepare_transfer(from.address(), amount, to_addr)?;
        let mut tx = unsigned.tx;
        let sig = Sig::from_bytes(from.sign(&unsigned.sighash));
        for i in 0..tx.inputs().len() {
            tx.attach_signature(i, sig);
        }
        Ok(tx)
    }

    /// Select covering UTXOs for `from` and build an **unsigned** transfer.
    /// One output is enough when it covers `amount + fee`; otherwise UTXOs are
    /// accumulated (largest first) until they do. The wallet signs `sighash`
    /// once and [`submit_signed`](Self::submit_signed) attaches it to every input
    /// (same owner).
    pub fn prepare_transfer(
        &self,
        from: Address,
        amount: u64,
        to: Address,
    ) -> Result<Prepared, NodeError> {
        if amount == 0 {
            return Err(NodeError::ZeroAmount);
        }
        let fee = self.min_fee();
        let need = amount
            .checked_add(fee)
            .ok_or(NodeError::InsufficientFunds)?;
        let state = self.ledger()?.ledger_state();
        let mut owned: Vec<(OutPoint, u64)> = state
            .iter()
            .filter(|(_, out)| out.owner == from)
            .map(|(op, out)| (*op, out.value))
            .collect();
        owned.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
        let mut selected: Vec<(OutPoint, u64)> = Vec::new();
        let mut total: u64 = 0;
        for (op, value) in owned {
            selected.push((op, value));
            total = total.saturating_add(value);
            if total >= need {
                break;
            }
        }
        if total < need {
            return Err(NodeError::InsufficientFunds);
        }
        let mut outputs = vec![TxOutput::new(amount, to)];
        let change = total - need;
        if change > 0 {
            outputs.push(TxOutput::new(change, from));
        }
        let outpoints: Vec<OutPoint> = selected.iter().map(|(op, _)| *op).collect();
        let tx = Transaction::unsigned(&outpoints, outputs, Vec::new());
        let sighash = tx.sighash();
        Ok(Prepared {
            tx,
            sighash,
            outpoint: selected[0].0,
            value: total,
            fee,
        })
    }

    /// Attach `signature` to a prepared transfer, verify it against `from`, and
    /// put the tx in the mempool. The secret key never enters the node.
    pub fn submit_signed(
        &mut self,
        from: Address,
        amount: u64,
        to: Address,
        signature: [u8; 64],
    ) -> Result<TxId, NodeError> {
        let prepared = self.prepare_transfer(from, amount, to)?;
        if !verify(&from, &prepared.sighash, &signature) {
            return Err(NodeError::BadSignature);
        }
        let mut tx = prepared.tx;
        let sig = Sig::from_bytes(signature);
        for i in 0..tx.inputs().len() {
            tx.attach_signature(i, sig);
        }
        self.submit_tx(tx)
    }

    /// Unspent outputs owned by `owner`.
    pub fn utxos_of(&self, owner: &Address) -> Result<Vec<(OutPoint, u64)>, NodeError> {
        let mut rows: Vec<(OutPoint, u64)> = self
            .ledger()?
            .ledger_state()
            .iter()
            .filter(|(_, o)| &o.owner == owner)
            .map(|(op, o)| (*op, o.value))
            .collect();
        rows.sort_by_key(|row| row.0);
        Ok(rows)
    }

    /// Send from a seed actor to an arbitrary address (faucet / demo).
    pub fn send_to(&mut self, from_seed: u64, amount: u64, to: Address) -> Result<Sent, NodeError> {
        let tx = self.build_transfer_to(from_seed, amount, to)?;
        let tx_id = tx.id();
        let parents = self.ledger()?.dag().tips();
        let timestamp = self.next_timestamp(self.ledger()?.dag(), &parents);
        let dag = self.ledger()?.dag();
        let work = dag.next_work_target(&parents).unwrap_or(1);
        let nonce = Self::mine_nonce(dag, &parents, work, timestamp, std::slice::from_ref(&tx));
        let ledger = self.ledger.as_mut().ok_or(NodeError::NotInitialized)?;
        let block = ledger
            .insert(parents, work, timestamp, nonce, &[tx])
            .map_err(NodeError::Insert)?;
        self.evict_mempool();
        Ok(Sent { block, tx: tx_id })
    }

    /// Send `amount` from actor `from_seed` to actor `to_seed` **immediately**,
    /// as a new block built on the current tips. (For the mempool flow use
    /// [`Node::pool`] then [`Node::produce_block`].)
    pub fn send(&mut self, from_seed: u64, amount: u64, to_seed: u64) -> Result<Sent, NodeError> {
        self.send_to(from_seed, amount, Self::address(to_seed))
    }

    /// Build a transfer and add it to the mempool (not yet in a block). Returns
    /// its transaction id.
    pub fn pool(&mut self, from_seed: u64, amount: u64, to_seed: u64) -> Result<TxId, NodeError> {
        let tx = self.build_transfer(from_seed, amount, to_seed)?;
        let id = tx.id();
        self.mempool.add(tx);
        Ok(id)
    }

    /// Accept an externally-formed transaction into the mempool (e.g. relayed by
    /// a peer). Rejects coinbase transactions. Returns its id.
    pub fn submit_tx(&mut self, tx: Transaction) -> Result<TxId, NodeError> {
        if tx.is_coinbase() {
            return Err(NodeError::UnexpectedCoinbase);
        }
        let id = tx.id();
        self.mempool.add(tx);
        Ok(id)
    }

    /// Assemble the largest valid prefix of the mempool into a block on the
    /// current tips, insert it, and drop the included transactions.
    ///
    /// Candidates are tried in deterministic (id) order against the current UTXO
    /// state; any that conflict are not included. After insert, the mempool
    /// evicts transactions whose inputs are gone from the selected-tip view
    /// (permanently invalid on this branch). Returns the new block id, or
    /// `None` if nothing could be included.
    pub fn produce_block(&mut self) -> Result<Option<BlockId>, NodeError> {
        if self.ledger.is_none() {
            return Err(NodeError::NotInitialized);
        }
        if self.mempool.is_empty() {
            return Ok(None);
        }

        let (subsidy, mut working, original) = {
            let ledger = self.ledger.as_ref().expect("checked above");
            (
                ledger.subsidy(),
                ledger.ledger_state(),
                ledger.ledger_state(),
            )
        };
        let mut selected = Vec::new();
        let mut selected_ids = Vec::new();
        for tx in self.mempool.ordered() {
            if apply_block(&mut working, std::slice::from_ref(&tx), subsidy).is_ok() {
                selected_ids.push(tx.id());
                selected.push(tx);
            }
        }
        if selected.is_empty() {
            return Ok(None);
        }
        let fees: u64 = selected.iter().map(|tx| fee_of(&original, tx)).sum();

        let dag = self.ledger.as_ref().expect("checked above").dag();
        let parents = dag.tips();
        let timestamp = self.next_timestamp(dag, &parents);
        let work = dag.next_work_target(&parents).unwrap_or(1);
        let mut block_txs = self.issuance_txs(timestamp, fees);
        block_txs.extend(selected);
        let nonce = Self::mine_nonce(dag, &parents, work, timestamp, &block_txs);
        let ledger = self.ledger.as_mut().expect("checked above");
        let block = ledger
            .insert(parents, work, timestamp, nonce, &block_txs)
            .map_err(NodeError::Insert)?;
        self.mempool.remove_all(&selected_ids);
        self.evict_mempool();
        Ok(Some(block))
    }

    /// Insert a block with no user transactions. If subsidy > 0, mints that many
    /// KVNC to the miner via coinbase — this is how supply grows after genesis.
    pub fn produce_empty(&mut self) -> Result<BlockId, NodeError> {
        let parents = self.ledger()?.dag().tips();
        let timestamp = self.next_timestamp(self.ledger()?.dag(), &parents);
        let dag = self.ledger()?.dag();
        let work = dag.next_work_target(&parents).unwrap_or(1);
        let txs = self.issuance_txs(timestamp, 0);
        let nonce = Self::mine_nonce(dag, &parents, work, timestamp, &txs);
        let ledger = self.ledger.as_mut().ok_or(NodeError::NotInitialized)?;
        let id = ledger
            .insert(parents, work, timestamp, nonce, &txs)
            .map_err(NodeError::Insert)?;
        self.evict_mempool();
        Ok(id)
    }

    /// Coinbase claiming subsidy + `extra_fees` for `miner`. Empty if nothing to mint.
    fn issuance_txs(&self, timestamp_ms: u64, extra_fees: u64) -> Vec<Transaction> {
        let Some(miner) = self.miner else {
            return Vec::new();
        };
        let subsidy = self.issuance().unwrap_or(0);
        let total = subsidy.saturating_add(extra_fees);
        if total == 0 {
            return Vec::new();
        }
        vec![Transaction::coinbase(
            vec![TxOutput::new(total, miner)],
            timestamp_ms.to_le_bytes().to_vec(),
        )]
    }

    /// A pending mempool transaction by id, if present.
    pub fn mempool_tx(&self, id: &TxId) -> Option<Transaction> {
        self.mempool.get(id)
    }

    fn evict_mempool(&mut self) {
        let Some(ledger) = self.ledger.as_ref() else {
            return;
        };
        let utxo = ledger.ledger_state();
        self.mempool.evict_invalid(&utxo);
    }

    /// The gossip record for a block, if present.
    pub fn block_record(&self, id: &BlockId) -> Option<BlockRecord> {
        let dag = self.ledger.as_ref()?.dag();
        let block = dag.block(id)?;
        let txs = decode_block_payload(block.payload()).ok()?;
        Some(BlockRecord {
            parents: block.parents().to_vec(),
            work: block.work(),
            timestamp_ms: block.timestamp_ms(),
            nonce: block.nonce(),
            txs,
        })
    }

    /// Every non-genesis block as a gossip record, in topological order — what a
    /// peer needs to catch up (genesis is shared out of band). Suitable to feed,
    /// in order, into [`Node::receive_block`] on another node.
    pub fn export(&self) -> Vec<BlockRecord> {
        let Some(ledger) = self.ledger.as_ref() else {
            return Vec::new();
        };
        let genesis = ledger.genesis();
        ledger
            .dag()
            .linearize()
            .into_iter()
            .filter(|id| *id != genesis)
            .filter_map(|id| self.block_record(&id))
            .collect()
    }

    /// Insert a block received from a peer. Idempotent: a block already present
    /// returns its id rather than an error. The block's parents must already be
    /// present (feed records in topological order).
    pub fn receive_block(&mut self, record: BlockRecord) -> Result<BlockId, NodeError> {
        // Node policy (not pure-DAG consensus): reject a block dated too far ahead
        // of our local wall clock. This depends on the local clock, so it cannot
        // live in `kovanica_dag`; it is applied here, before the ledger insert.
        let now_ms = self.now_ms();
        if record.timestamp_ms > now_ms.saturating_add(MAX_FUTURE_DRIFT_MS) {
            return Err(NodeError::TimestampTooFarInFuture {
                timestamp_ms: record.timestamp_ms,
                now_ms,
            });
        }
        let ledger = self.ledger.as_mut().ok_or(NodeError::NotInitialized)?;
        match ledger.insert(
            record.parents,
            record.work,
            record.timestamp_ms,
            record.nonce,
            &record.txs,
        ) {
            Ok(id) => {
                self.evict_mempool();
                Ok(id)
            }
            Err(LedgerInsertError::Dag(DagError::DuplicateBlock(id))) => Ok(id),
            Err(e) => Err(NodeError::Insert(e)),
        }
    }

    /// Write the ledger snapshot to `path`.
    pub fn save(&self, path: &str) -> Result<(), NodeError> {
        let bytes = self.ledger()?.write_snapshot();
        fs::write(path, bytes).map_err(|e| NodeError::Io(e.to_string()))
    }

    /// Replace the node's ledger with one loaded from the snapshot at `path`.
    pub fn load(&mut self, path: &str) -> Result<(), NodeError> {
        let bytes = fs::read(path).map_err(|e| NodeError::Io(e.to_string()))?;
        let ledger =
            Ledger::read_snapshot(&bytes).map_err(|e| NodeError::Snapshot(e.to_string()))?;
        self.ledger = Some(ledger);
        Ok(())
    }

    /// Write an incremental append-only log of this node's ledger at `path`.
    pub fn create_log(&self, path: &str) -> Result<LedgerStore, NodeError> {
        LedgerStore::create(path, self.ledger()?).map_err(|e| NodeError::Io(e.to_string()))
    }

    /// Rebuild the node from an incremental log at `path`. The store is
    /// returned so the caller can [`persist_block`](Self::persist_block) new
    /// inserts without rewriting the file.
    pub fn load_log(path: &str) -> Result<(Self, LedgerStore), NodeError> {
        let (store, ledger) =
            LedgerStore::open(path).map_err(|e| NodeError::Snapshot(e.to_string()))?;
        Ok((
            Self {
                ledger: Some(ledger),
                mempool: Mempool::new(),
                clock: Clock::default(),
                miner: None,
            },
            store,
        ))
    }

    /// Append `id`'s block to an open log. No-op-level error if the block is
    /// missing (it must already be in this node).
    pub fn persist_block(&self, store: &mut LedgerStore, id: &BlockId) -> Result<(), NodeError> {
        let block = self
            .ledger()?
            .dag()
            .block(id)
            .ok_or_else(|| NodeError::Io("unknown block".into()))?;
        store
            .append(block)
            .map_err(|e| NodeError::Io(e.to_string()))
    }
}

fn fee_of(state: &UtxoSet, tx: &Transaction) -> u64 {
    let mut sum_in = 0u64;
    for input in tx.inputs() {
        if let Some(prev) = state.get(&input.outpoint) {
            sum_in = sum_in.saturating_add(prev.value);
        }
    }
    let sum_out: u64 = tx.outputs().iter().map(|o| o.value).sum();
    sum_in.saturating_sub(sum_out)
}
