//! The append-only block DAG store and its GHOSTDAG metadata.
//!
//! The [`Dag`] owns every block plus the consensus data GHOSTDAG derives for it
//! (see [`crate::ghostdag`]) and the total order it induces (see
//! [`crate::ordering`]). Blocks are inserted one at a time; each insert is
//! validated and immediately coloured, so the store always holds a fully
//! processed DAG.
//!
//! ## Reachability
//!
//! Ancestor queries and mergeset computation go through a [`Reachability`]
//! oracle (interval-labelled selected-parent tree + future-covering sets), so no
//! per-block `past` set is stored — each block keeps only its `past_size` (the
//! *count* of its ancestors), which is enough for the topological sort key. The
//! oracle is maintained **incrementally**: each insert folds in just the one new
//! block (Kaspa reachability / interval reindexing) rather than rebuilding from
//! scratch (see [`crate::reachability`]).

use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};

use crate::block::{Block, BlockId};
use crate::difficulty::{Retarget, TimedWork};
use crate::reachability::Reachability;
use crate::validation::BlockValidator;

/// Errors returned when inserting a block into the [`Dag`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DagError {
    /// A block with this id is already present.
    DuplicateBlock(BlockId),
    /// A referenced parent is not in the DAG.
    MissingParent(BlockId),
    /// A non-genesis block referenced no parents.
    NoParents(BlockId),
    /// `insert_genesis` was called on a DAG that already has a genesis.
    GenesisAlreadySet,
    /// The installed [`BlockValidator`] rejected the block, with its reason.
    InvalidBlock { id: BlockId, reason: String },
    /// Difficulty is enforced and the block's `work` does not equal the target
    /// its past implies (see [`Dag::set_difficulty`] and [`crate::difficulty`]).
    DifficultyMismatch {
        id: BlockId,
        expected: u128,
        actual: u128,
    },
    /// Difficulty is enforced and the block's timestamp precedes a parent's —
    /// a block may not be older than a block it builds on.
    NonMonotonicTimestamp {
        id: BlockId,
        timestamp_ms: u64,
        parent_timestamp_ms: u64,
    },
    /// Proof-of-work is enforced (see [`Dag::set_proof_of_work`]) and the block's
    /// id does not meet its `work` target — it was not adequately mined.
    InsufficientProofOfWork { id: BlockId, work: u128 },
}

impl core::fmt::Display for DagError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            DagError::DuplicateBlock(id) => write!(f, "duplicate block {id}"),
            DagError::MissingParent(id) => write!(f, "missing parent {id}"),
            DagError::NoParents(id) => write!(f, "non-genesis block {id} has no parents"),
            DagError::GenesisAlreadySet => write!(f, "genesis already set"),
            DagError::InvalidBlock { id, reason } => {
                write!(f, "block {id} rejected by validator: {reason}")
            }
            DagError::DifficultyMismatch {
                id,
                expected,
                actual,
            } => write!(
                f,
                "block {id} has work {actual}, but difficulty requires {expected}"
            ),
            DagError::NonMonotonicTimestamp {
                id,
                timestamp_ms,
                parent_timestamp_ms,
            } => write!(
                f,
                "block {id} timestamp {timestamp_ms}ms precedes parent timestamp {parent_timestamp_ms}ms"
            ),
            DagError::InsufficientProofOfWork { id, work } => write!(
                f,
                "block {id} does not meet its proof-of-work target for work {work}"
            ),
        }
    }
}

impl std::error::Error for DagError {}

/// The `k` parameter of GHOSTDAG: the maximum tolerated blue anticone size.
///
/// It bounds how many well-connected blocks may be mutually "parallel" (in each
/// other's anticone) while still all counting as blue. Larger `k` tolerates
/// higher block rates / latency at the cost of a wider security margin.
pub type KParam = u16;

/// Consensus metadata GHOSTDAG derives for a single block.
///
/// A block's *blue set* is the set of blue blocks in its past. It is built from
/// its selected parent's blue set plus the blues found in its mergeset.
#[derive(Clone, Debug)]
pub struct GhostdagData {
    /// The parent with the heaviest blue work (see [`Dag::chain_key`]).
    /// `None` only for genesis.
    pub selected_parent: Option<BlockId>,
    /// Mergeset blocks coloured blue, in the topological order they were added.
    pub mergeset_blues: Vec<BlockId>,
    /// Mergeset blocks coloured red, in topological order.
    pub mergeset_reds: Vec<BlockId>,
    /// Size of this block's blue set (number of blue blocks in its past).
    pub blue_score: u64,
    /// Total work of this block's blue set.
    pub blue_work: u128,
    /// For each blue block in this block's blue set, the number of blue blocks
    /// in *its* anticone (restricted to this blue set). The invariant GHOSTDAG
    /// maintains is that every value here is `<= k`.
    pub blue_anticone_sizes: HashMap<BlockId, KParam>,
}

/// The GHOSTDAG data a block *would* receive, computed by [`Dag::preview`]
/// without inserting the block.
#[derive(Clone, Debug)]
pub struct BlockPreview {
    /// The parent that would be the block's selected parent.
    pub selected_parent: BlockId,
    /// The block's mergeset, in the deterministic order the linearization uses.
    pub mergeset: Vec<BlockId>,
}

/// A stored block: the block itself plus derived DAG/consensus data.
pub(crate) struct Node {
    pub(crate) block: Block,
    /// Number of strict ancestors of this block (`|past|`). The full set is not
    /// stored — reachability comes from the oracle — but the count is the
    /// topological sort key and is maintained in O(1): `past_size(sp) + 1 +
    /// |mergeset|`.
    pub(crate) past_size: u64,
    /// Direct children, for tip maintenance and total-order traversal.
    pub(crate) children: BTreeSet<BlockId>,
    pub(crate) ghostdag: GhostdagData,
}

/// An append-only block DAG with GHOSTDAG consensus data.
pub struct Dag {
    k: KParam,
    genesis: BlockId,
    pub(crate) nodes: HashMap<BlockId, Node>,
    /// Blocks with no children yet — the current tips.
    tips: BTreeSet<BlockId>,
    /// Reachability oracle backing `is_ancestor` and mergeset computation,
    /// maintained incrementally on each insert (see [`crate::reachability`]).
    reach: Reachability,
    /// Optional payload-aware validator run on each [`Dag::insert`]. See
    /// [`crate::validation`].
    validator: Option<Box<dyn BlockValidator>>,
    /// Optional consensus-enforced difficulty policy. When set, each
    /// [`Dag::insert`] requires the block's `work` to equal the target its past
    /// implies and its timestamp not to precede any parent's. See
    /// [`Dag::set_difficulty`] and [`crate::difficulty`].
    difficulty: Option<Retarget>,
    /// Consensus-enforced proof-of-work switch. When `true`, each [`Dag::insert`]
    /// requires every non-genesis block's id to meet its `work` target (see
    /// [`crate::pow`] and [`Dag::set_proof_of_work`]). Off by default.
    require_pow: bool,
}

impl Dag {
    /// Create a DAG seeded with `genesis` and the GHOSTDAG parameter `k`.
    pub fn new(k: KParam, genesis: Block) -> Self {
        let genesis_id = genesis.id();
        let mut nodes = HashMap::new();
        nodes.insert(
            genesis_id,
            Node {
                block: genesis,
                past_size: 0,
                children: BTreeSet::new(),
                ghostdag: GhostdagData {
                    selected_parent: None,
                    mergeset_blues: Vec::new(),
                    mergeset_reds: Vec::new(),
                    blue_score: 0,
                    blue_work: 0,
                    blue_anticone_sizes: HashMap::new(),
                },
            },
        );
        let mut tips = BTreeSet::new();
        tips.insert(genesis_id);
        let mut dag = Self {
            k,
            genesis: genesis_id,
            nodes,
            tips,
            reach: Reachability::empty(),
            validator: None,
            difficulty: None,
            require_pow: false,
        };
        dag.reach = Reachability::build(&dag);
        dag
    }

    /// Create a DAG as [`Dag::new`] but with a [`BlockValidator`] installed, so
    /// every subsequent [`Dag::insert`] must pass `validator`. The genesis block
    /// itself is not validated.
    pub fn with_validator(k: KParam, genesis: Block, validator: Box<dyn BlockValidator>) -> Self {
        let mut dag = Self::new(k, genesis);
        dag.validator = Some(validator);
        dag
    }

    /// Install (or replace) the block validator run on every [`Dag::insert`].
    pub fn set_validator(&mut self, validator: Box<dyn BlockValidator>) {
        self.validator = Some(validator);
    }

    /// Enable consensus-enforced difficulty with retargeting policy `retarget`.
    ///
    /// Once enabled, every subsequent [`Dag::insert`] of a non-genesis block
    /// must satisfy both rules (see [`crate::difficulty`]):
    ///
    /// * **Enforced work.** The block's `work` must equal
    ///   `retarget.next_work(samples)`, where `samples` are the last
    ///   `window + 1` blocks of the selected-parent chain ending at the block's
    ///   selected parent (oldest first). Because the samples and the selected
    ///   chain are a pure function of the DAG, every node computes the same
    ///   target, so this is a deterministic consensus rule. Blocks with too
    ///   little history are required to carry [`Retarget::min_work`].
    /// * **Monotone timestamp.** The block's timestamp must not be earlier than
    ///   any parent's, so timestamps along every path are non-decreasing and the
    ///   retarget's timespans are well defined.
    ///
    /// Genesis is exempt (it has no past). Difficulty is off by default, so a
    /// DAG built without this call accepts any `work`, exactly as before.
    ///
    /// Note: this enforces work against the target the DAG implies; it does
    /// **not** bound a timestamp against wall-clock time (a "not too far in the
    /// future" rule is node policy, not a pure function of the DAG, and remains a
    /// follow-up).
    pub fn set_difficulty(&mut self, retarget: Retarget) {
        self.difficulty = Some(retarget);
    }

    /// The enforced difficulty policy, if any (see [`Dag::set_difficulty`]).
    pub fn difficulty(&self) -> Option<Retarget> {
        self.difficulty
    }

    /// Enable (or disable) consensus-enforced proof-of-work.
    ///
    /// Once enabled, every subsequent [`Dag::insert`] of a non-genesis block
    /// requires the block's id to meet its `work` target — i.e. it must have been
    /// **mined** so that `H * work < 2^256`, where `H` is the id as a big-endian
    /// 256-bit integer (Nakamoto-style hash-target PoW; see [`crate::pow`]).
    /// Genesis is exempt (it is a fixed anchor, not mined).
    ///
    /// Verification is a pure function of the block, so every node agrees. PoW is
    /// **off by default**, so a DAG built without this call accepts any nonce,
    /// exactly as before. It composes with [`Dag::set_difficulty`]: with both on,
    /// difficulty pins `work` to [`Dag::next_work_target`] *and* the block must be
    /// mined to meet that work's target.
    pub fn set_proof_of_work(&mut self, enabled: bool) {
        self.require_pow = enabled;
    }

    /// Whether consensus-enforced proof-of-work is on (see
    /// [`Dag::set_proof_of_work`]).
    pub fn proof_of_work_enabled(&self) -> bool {
        self.require_pow
    }

    /// The `work` a new block built on `parents` must carry to satisfy the
    /// enforced difficulty policy, or `None` when difficulty is disabled.
    ///
    /// This is the miner's counterpart to insert-time enforcement: mine a block
    /// with this work (and a timestamp not preceding any parent's) and it passes
    /// [`Dag::insert`]'s difficulty check. `parents` must be present in the DAG.
    pub fn next_work_target(&self, parents: &[BlockId]) -> Option<u128> {
        let retarget = self.difficulty?;
        let sp = parents.iter().copied().max_by_key(|p| self.chain_key(p))?;
        Some(retarget.next_work(&self.chain_samples(sp, retarget.window)))
    }

    /// The GHOSTDAG `k` parameter.
    pub fn k(&self) -> KParam {
        self.k
    }

    /// The genesis block id.
    pub fn genesis(&self) -> BlockId {
        self.genesis
    }

    /// Number of blocks in the DAG (including genesis).
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Whether the DAG only contains genesis.
    pub fn is_empty(&self) -> bool {
        self.nodes.len() <= 1
    }

    /// Whether a block is present.
    pub fn contains(&self, id: &BlockId) -> bool {
        self.nodes.contains_key(id)
    }

    /// The current tips (blocks with no children), sorted by id.
    pub fn tips(&self) -> Vec<BlockId> {
        self.tips.iter().copied().collect()
    }

    /// Borrow a stored block.
    pub fn block(&self, id: &BlockId) -> Option<&Block> {
        self.nodes.get(id).map(|n| &n.block)
    }

    /// Borrow the GHOSTDAG data derived for a block.
    pub fn ghostdag(&self, id: &BlockId) -> Option<&GhostdagData> {
        self.nodes.get(id).map(|n| &n.ghostdag)
    }

    /// `true` iff `ancestor` is a strict ancestor of `descendant`
    /// (i.e. `ancestor` is in `descendant`'s past). `false` for equal ids.
    ///
    /// Answered by the [`Reachability`] oracle in O(1)/O(fcs) rather than from a
    /// stored past set.
    pub fn is_ancestor(&self, ancestor: &BlockId, descendant: &BlockId) -> bool {
        self.reach.is_ancestor(ancestor, descendant)
    }

    /// Amortisation metrics `(reindexes, relayout_touches)` of the backing
    /// reachability oracle: how many interval reindexes its incremental
    /// maintenance has performed and how many tree nodes those reindexes touched.
    /// Pure bookkeeping that never affects a query answer — exposed so tests can
    /// prove interval reindexing stays cheaply amortised (see
    /// [`Reachability::reindex_metrics`]).
    pub fn reachability_reindex_metrics(&self) -> (u64, u64) {
        self.reach.reindex_metrics()
    }

    /// `true` iff `a` and `b` are in each other's anticone: distinct blocks
    /// where neither is an ancestor of the other (they are "parallel").
    pub fn in_anticone(&self, a: &BlockId, b: &BlockId) -> bool {
        a != b && !self.is_ancestor(a, b) && !self.is_ancestor(b, a)
    }

    /// The chain-selection key used to rank blocks: heavier blue work wins,
    /// then higher blue score, then larger id as a deterministic final tiebreak.
    ///
    /// Used to pick a block's selected parent and the DAG's selected tip.
    pub(crate) fn chain_key(&self, id: &BlockId) -> (u128, u64, BlockId) {
        let g = &self.nodes[id].ghostdag;
        (g.blue_work, g.blue_score, *id)
    }

    /// The mergeset for a block with selected parent `sp` and the given `parents`,
    /// in deterministic topological order: `past(block) \ (past(sp) ∪ {sp})`,
    /// sorted by `(past_size, id)`.
    ///
    /// Computed by a backward walk over parent edges from `parents`, bounded by
    /// `sp`'s past (a block in `past(sp) ∪ {sp}` is a boundary — not merged, and
    /// its ancestors, all also in `past(sp)`, are not traversed). Reachability is
    /// the oracle. Shared by GHOSTDAG colouring, the linearization, and
    /// [`Dag::preview`] so all three agree on the mergeset and its order.
    pub(crate) fn mergeset_ordered(&self, sp: BlockId, parents: &[BlockId]) -> Vec<BlockId> {
        let mut mergeset: Vec<BlockId> = Vec::new();
        let mut seen: HashSet<BlockId> = HashSet::new();
        let mut queue: VecDeque<BlockId> = parents.iter().copied().collect();
        while let Some(x) = queue.pop_front() {
            if !seen.insert(x) {
                continue;
            }
            if x == sp || self.is_ancestor(&x, &sp) {
                continue; // x ∈ past(sp) ∪ {sp}: boundary
            }
            mergeset.push(x);
            for parent in self.nodes[&x].block.parents() {
                queue.push_back(*parent);
            }
        }
        // Topological order: a strict ancestor has a strictly smaller past_size.
        mergeset.sort_by_key(|b| (self.nodes[b].past_size, *b));
        mergeset
    }

    /// Enforce the difficulty rules on a prospective `block` (id `id`) with
    /// selected parent `sp`, under policy `retarget`. See [`Dag::set_difficulty`].
    fn check_difficulty(
        &self,
        block: &Block,
        id: BlockId,
        sp: BlockId,
        retarget: &Retarget,
    ) -> Result<(), DagError> {
        // Timestamp must not precede any parent's (monotone along every path).
        for parent in block.parents() {
            let parent_ts = self.nodes[parent].block.timestamp_ms();
            if block.timestamp_ms() < parent_ts {
                return Err(DagError::NonMonotonicTimestamp {
                    id,
                    timestamp_ms: block.timestamp_ms(),
                    parent_timestamp_ms: parent_ts,
                });
            }
        }

        // Work must equal the target the selected chain ending at `sp` implies.
        let expected = retarget.next_work(&self.chain_samples(sp, retarget.window));
        if block.work() != expected {
            return Err(DagError::DifficultyMismatch {
                id,
                expected,
                actual: block.work(),
            });
        }
        Ok(())
    }

    /// The last `window + 1` blocks of the selected-parent chain ending at `tip`
    /// (inclusive), oldest first, as difficulty-retarget samples. This is the
    /// window [`Retarget::next_work`] scores to set the *next* block's work.
    fn chain_samples(&self, tip: BlockId, window: usize) -> Vec<TimedWork> {
        let mut samples = Vec::new();
        let mut cur = Some(tip);
        while let Some(id) = cur {
            let node = &self.nodes[&id];
            samples.push(TimedWork::new(node.block.timestamp_ms(), node.block.work()));
            if samples.len() == window + 1 {
                break;
            }
            cur = node.ghostdag.selected_parent;
        }
        samples.reverse(); // collected newest-first; retarget wants oldest-first
        samples
    }

    /// Preview the GHOSTDAG selected parent and mergeset a block would get if it
    /// were inserted with `block`'s parents — **without** inserting it.
    ///
    /// Runs the same structural checks as [`Dag::insert`] (duplicate, no parents,
    /// missing parent) so a caller can validate a prospective block against its
    /// view before committing it. This is what lets the state layer apply a
    /// block's transactions on top of its selected parent's UTXO state and reject
    /// an invalid block before it enters the DAG.
    pub fn preview(&self, block: &Block) -> Result<BlockPreview, DagError> {
        let id = block.id();
        if self.nodes.contains_key(&id) {
            return Err(DagError::DuplicateBlock(id));
        }
        if block.parents().is_empty() {
            return Err(DagError::NoParents(id));
        }
        for parent in block.parents() {
            if !self.nodes.contains_key(parent) {
                return Err(DagError::MissingParent(*parent));
            }
        }
        let selected_parent = *block
            .parents()
            .iter()
            .max_by_key(|p| self.chain_key(p))
            .expect("non-empty parents");
        let mergeset = self.mergeset_ordered(selected_parent, block.parents());
        Ok(BlockPreview {
            selected_parent,
            mergeset,
        })
    }

    /// Insert `block`, validating and colouring it. Returns its id.
    ///
    /// Fails if the block is a duplicate, references a missing parent, (for a
    /// non-genesis block) references no parents, is rejected by the installed
    /// [`BlockValidator`] (if any), or — when difficulty is enforced (see
    /// [`Dag::set_difficulty`]) — carries the wrong `work` or a timestamp that
    /// precedes a parent's. The structural DAG checks run first, so a validator
    /// only ever sees a block whose parents are present.
    pub fn insert(&mut self, block: Block) -> Result<BlockId, DagError> {
        let id = block.id();
        if self.nodes.contains_key(&id) {
            return Err(DagError::DuplicateBlock(id));
        }
        if block.parents().is_empty() {
            return Err(DagError::NoParents(id));
        }
        for parent in block.parents() {
            if !self.nodes.contains_key(parent) {
                return Err(DagError::MissingParent(*parent));
            }
        }

        // Payload-aware validation, before the block is added to the DAG. Both
        // borrows of `self` here are shared, which the borrow checker allows.
        if let Some(validator) = self.validator.as_deref() {
            validator
                .validate(&block, self)
                .map_err(|reason| DagError::InvalidBlock { id, reason })?;
        }

        // Derive GHOSTDAG data (selected parent, mergeset, colouring) against the
        // oracle as it stands before this block is added.
        let ghostdag = self.compute_ghostdag(block.parents());

        // past_size(B) = past_size(sp) + 1 + |mergeset(B)| (a disjoint union).
        let sp = ghostdag
            .selected_parent
            .expect("non-genesis has a selected parent");

        // Consensus-enforced difficulty, if enabled: the block's timestamp must
        // not precede a parent's, and its work must equal the target its past
        // (the selected chain ending at `sp`) implies. Checked before the block
        // is wired in, so a rejected block leaves the DAG unchanged.
        if let Some(retarget) = self.difficulty {
            self.check_difficulty(&block, id, sp, &retarget)?;
        }

        // Consensus-enforced proof-of-work, if enabled: the block's id must meet
        // its `work` target (Nakamoto-style hash-target PoW; see `crate::pow`).
        // Genesis is exempt, but this path only runs for non-genesis inserts.
        // Independent of and composable with the difficulty check above.
        if self.require_pow && !crate::pow::meets_target(&id, block.work()) {
            return Err(DagError::InsufficientProofOfWork {
                id,
                work: block.work(),
            });
        }
        let past_size = self.nodes[&sp].past_size
            + 1
            + (ghostdag.mergeset_blues.len() + ghostdag.mergeset_reds.len()) as u64;

        // The full mergeset (blues + reds), captured before `ghostdag` is moved
        // into the node — this is what the oracle needs to update its
        // future-covering sets.
        let mergeset: Vec<BlockId> = ghostdag
            .mergeset_blues
            .iter()
            .chain(&ghostdag.mergeset_reds)
            .copied()
            .collect();

        // Wire the block in: attach to parents, refresh tips.
        for parent in block.parents() {
            self.nodes.get_mut(parent).unwrap().children.insert(id);
            self.tips.remove(parent);
        }
        self.tips.insert(id);

        self.nodes.insert(
            id,
            Node {
                block,
                past_size,
                children: BTreeSet::new(),
                ghostdag,
            },
        );

        // Fold the one new block into the reachability oracle incrementally
        // (Kaspa reachability / interval reindexing), rather than rebuilding it.
        self.reach.add_block(id, sp, &mergeset);
        Ok(id)
    }
}
