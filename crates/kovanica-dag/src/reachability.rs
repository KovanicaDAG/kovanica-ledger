//! A reachability oracle: answer "is A an ancestor of B?" without a full
//! per-block `past` set.
//!
//! Today [`Dag`] answers reachability from this oracle rather than from a
//! per-block `past` set stored in full — O(1)/O(fcs) per query but only O(n)
//! memory. This is the standard structure as used by **Kaspa** (see
//! `rusty-kaspa`'s `consensus::processes::reachability`): a **reachability
//! tree** with interval labels, plus a **future-covering set** per block for the
//! DAG edges the tree does not capture.
//!
//! ## How it works
//!
//! * The **reachability tree** is the selected-parent tree (each block's tree
//!   parent is its GHOSTDAG selected parent; genesis is the root). Every node
//!   carries an interval `[start, end]` such that the subtree of a node is
//!   exactly the contiguous range `[start, end]` and the node itself sits at
//!   `start`. So `X` is a tree-ancestor of (or equal to) `Y` iff
//!   `X.start <= Y.start <= X.end` — an O(1) check.
//!
//! * DAG reachability has edges the tree misses (a block's non-selected
//!   parents). For each block `A` the **future-covering set** `fcs(A)` holds the
//!   tree-roots of `future(A) \ tree_subtree(A)` — the minimal blocks whose tree
//!   subtrees cover the rest of `A`'s future. Then `A` reaches `B` iff `B` is in
//!   `A`'s tree subtree, or `B` is in the tree subtree of some `fcs(A)` member.
//!
//! ## Incremental maintenance (Kaspa reachability / interval reindexing)
//!
//! The oracle is maintained **incrementally**: each [`Dag::insert`] calls
//! [`Reachability::add_block`] to fold in exactly the one new block, never
//! rebuilding from scratch. Because the DAG is append-only and a block's
//! GHOSTDAG selected parent is fixed at insert, the reachability tree only ever
//! gains a **new leaf** under an already-present parent — existing tree edges
//! never move. So `add_block` is Kaspa's two-step `add_block`:
//!
//! 1. **Tree interval allocation with reindexing.** The new block is carved a
//!    sub-interval out of its selected parent's *remaining* capacity (Kaspa
//!    splits the remaining span in half and gives the child the first half, so a
//!    fresh leaf still gets room for its own future subtree — here additionally
//!    **capped at [`CHILD_RESERVE`]** so a wide fan cannot exhaust a large parent
//!    interval after only `log2(width)` children, the interval-reindex
//!    amortisation tuning). When the parent has
//!    no spare capacity, a **reindex** re-lays-out the minimal enclosing subtree
//!    — the lowest ancestor whose interval is roomy enough — distributing that
//!    interval among its children in proportion to subtree size (with slack), so
//!    the exhausted branch gets space again. A reindex touches only that
//!    subtree; the ancestor's own interval is unchanged, so blocks outside it are
//!    untouched. This is Kaspa's interval-reindexing scheme (a simplified
//!    reindex-root policy: pick the lowest sufficiently-large ancestor).
//!
//! 2. **Future-covering-set insertion.** For each block in the new block's
//!    **mergeset** (the selected parent's anticone that the new block merges),
//!    the new block is inserted into that block's future-covering set, kept
//!    minimal and sorted by interval (Kaspa's `insert_to_future_covering_set`:
//!    skip if an existing entry already covers the new block, drop entries the
//!    new block covers, else insert in interval order). Only the *direct*
//!    mergeset needs updating: a deeper ancestor `A` of a merged block already
//!    covers the whole selected-parent subtree via an existing fcs entry that
//!    points into the selected chain, and the new leaf lands inside that same
//!    subtree.
//!
//! [`Reachability::build`] (from-scratch construction over a whole [`Dag`]) is
//! retained: it seeds genesis and is an independent oracle in the differential
//! tests, where the incrementally-maintained oracle is checked, after *every*
//! insert, to give identical query answers to a freshly-built one (see
//! `tests/reachability.rs`). Exact interval numbers differ between the two paths;
//! only the containment answers matter, and those are asserted equal.

use std::collections::{HashMap, HashSet, VecDeque};

use crate::block::BlockId;
use crate::dag::Dag;

/// The interval span handed to the genesis (tree-root) block. Large enough that
/// reindexes are rare, small enough (half the `u64` range) that the interval
/// arithmetic below never overflows.
const ROOT_CAPACITY: u64 = u64::MAX >> 1;

/// Amortisation cap on the interval span a single freshly-allocated child is
/// handed (both in incremental allocation and in a reindex re-layout).
///
/// Kaspa's raw rule gives a new child **half** its parent's *remaining* interval,
/// so that a leaf that later grows a subtree already has room. That is right for a
/// chain (the one child inherits a geometric share and can grow deep), but
/// pathological for a **wide fan**: under a single parent, every leaf child eats
/// half the remaining span, so the parent exhausts its capacity after only
/// `log2(width)` children — then each further child forces a reindex that
/// re-lays-out *all* the parent's children (the relayout distributes the whole
/// interval among them, leaving the parent almost no reserve, so it re-exhausts
/// almost at once). The result is `O(children^2)` reindex work for a broad fan.
///
/// Capping a child's span at `CHILD_RESERVE` breaks that: a leaf still gets a
/// generous fixed reserve to grow its own subtree, but no longer swallows an
/// exponential share of a large interval, so a parent with a large interval can
/// host up to `interval_width / CHILD_RESERVE` children before it ever needs a
/// reindex. Genesis's interval is `ROOT_CAPACITY` (~2^63), so with this cap a
/// direct fan off genesis absorbs ~2^23 (8 million) children reindex-free, while a
/// chain is unaffected (half of a `CHILD_RESERVE`-wide interval is below the cap,
/// so deep structures still split geometrically and reindex only locally). The cap
/// is purely an interval-*numbering* choice: it changes which integers label the
/// tree, never which block contains which, so every reachability query answer is
/// identical.
const CHILD_RESERVE: u64 = 1 << 40;

/// An interval-labelled reachability tree plus per-block future-covering sets.
///
/// The `intervals`/`fcs` pair backs the public queries; the remaining fields are
/// the persistent bookkeeping that lets [`Reachability::add_block`] maintain them
/// incrementally (Kaspa reachability / interval reindexing).
#[derive(Clone, Debug)]
pub struct Reachability {
    /// Tree interval `[start, end]` per block: the block's subtree is the
    /// contiguous range `[start, end]` of `start` labels, and the block sits at
    /// `start`.
    intervals: HashMap<BlockId, (u64, u64)>,
    /// Future-covering set per block: the tree-roots of `future(b) \ subtree(b)`,
    /// sorted by interval start.
    fcs: HashMap<BlockId, Vec<BlockId>>,
    /// Selected-parent-tree children per block, sorted by [`BlockId`] for
    /// deterministic re-layout. The oracle's reachability tree.
    tree_children: HashMap<BlockId, Vec<BlockId>>,
    /// Tree parent (GHOSTDAG selected parent) per block; absent for the root.
    /// Used to walk up when searching for a reindex ancestor.
    tree_parent: HashMap<BlockId, BlockId>,
    /// Number of blocks in each block's tree subtree (itself included). Maintained
    /// in O(depth) per insert; sizes the proportional re-layout on a reindex.
    subtree_size: HashMap<BlockId, u64>,
    /// Next free interval start within each block's child region `[start+1, end]`
    /// — i.e. the low end of its still-unallocated capacity.
    next_free: HashMap<BlockId, u64>,
    /// Amortisation metric: how many interval **reindexes** [`Reachability::add_block`]
    /// has triggered over this oracle's life. A reindex re-lays-out a subtree, so
    /// this is the count of the expensive events; a lower value for the same
    /// insertion sequence means better amortisation. Query-irrelevant bookkeeping.
    reindexes: u64,
    /// Amortisation metric: the total number of tree nodes re-laid-out across all
    /// reindexes (summed subtree sizes). This is the actual work reindexing has
    /// done — the headline cost the amortisation tuning drives down.
    relayout_touches: u64,
}

impl Reachability {
    /// An empty oracle (no blocks). Used only to seed a [`Dag`] before the first
    /// [`Reachability::build`].
    pub(crate) fn empty() -> Self {
        Self {
            intervals: HashMap::new(),
            fcs: HashMap::new(),
            tree_children: HashMap::new(),
            tree_parent: HashMap::new(),
            subtree_size: HashMap::new(),
            next_free: HashMap::new(),
            reindexes: 0,
            relayout_touches: 0,
        }
    }

    /// Build the oracle from `dag`'s structure (the blocks, their parents, and
    /// their GHOSTDAG selected parents), from scratch.
    ///
    /// This iterates the DAG's nodes directly rather than calling `linearize()`,
    /// which — now that the DAG is oracle-backed — would recurse into the oracle
    /// being built. It produces a fully-consistent oracle, including the
    /// bookkeeping [`Reachability::add_block`] later extends, so a genesis-only
    /// build is a valid seed for incremental growth.
    pub fn build(dag: &Dag) -> Self {
        // Reachability tree = selected-parent tree. Collect tree children/parents
        // and the DAG children (inverse of parents, for walking futures).
        let mut tree_children: HashMap<BlockId, Vec<BlockId>> = HashMap::new();
        let mut tree_parent: HashMap<BlockId, BlockId> = HashMap::new();
        let mut dag_children: HashMap<BlockId, Vec<BlockId>> = HashMap::new();
        for (id, node) in &dag.nodes {
            tree_children.entry(*id).or_default();
            if let Some(sp) = node.ghostdag.selected_parent {
                tree_children.entry(sp).or_default().push(*id);
                tree_parent.insert(*id, sp);
            }
            for parent in node.block.parents() {
                dag_children.entry(*parent).or_default().push(*id);
            }
        }
        // Deterministic child order → deterministic interval labels.
        for children in tree_children.values_mut() {
            children.sort_unstable();
        }

        let genesis = dag.genesis();
        let subtree_size = subtree_sizes(genesis, &tree_children);

        let mut me = Self {
            intervals: HashMap::new(),
            fcs: HashMap::new(),
            tree_children,
            tree_parent,
            subtree_size,
            next_free: HashMap::new(),
            reindexes: 0,
            relayout_touches: 0,
        };
        // Lay out the whole tree under a generous root interval, so there is room
        // for later incremental inserts before the first reindex.
        me.relayout_subtree(genesis, 1, ROOT_CAPACITY);
        me.fcs = build_fcs(dag, &dag_children, &me.intervals);
        me
    }

    /// Fold one freshly-inserted block into the oracle (Kaspa's incremental
    /// `add_block`): allocate its tree interval under `selected_parent`
    /// (reindexing the minimal enclosing subtree if the parent is out of
    /// capacity) and insert it into the future-covering set of every block in
    /// `mergeset`.
    ///
    /// `id` must be new, `selected_parent` already present, and `mergeset` the
    /// block's full mergeset (the selected parent's anticone it merges). The DAG
    /// is append-only and `selected_parent` is fixed, so `id` is always a new
    /// **leaf** of the reachability tree — existing tree edges never move.
    pub(crate) fn add_block(
        &mut self,
        id: BlockId,
        selected_parent: BlockId,
        mergeset: &[BlockId],
    ) {
        // 1. Register the new leaf in the tree and bump ancestor subtree sizes.
        let children = self.tree_children.entry(selected_parent).or_default();
        // Keep children sorted by id (deterministic re-layout order).
        let pos = children.partition_point(|c| *c < id);
        children.insert(pos, id);
        self.tree_children.entry(id).or_default();
        self.tree_parent.insert(id, selected_parent);
        self.subtree_size.insert(id, 1);
        let mut cur = Some(selected_parent);
        while let Some(x) = cur {
            *self.subtree_size.get_mut(&x).expect("ancestor sized") += 1;
            cur = self.tree_parent.get(&x).copied();
        }

        // 2. Allocate `id`'s tree interval out of its parent's remaining capacity.
        //    Kaspa splits the remaining span in half and gives the child the first
        //    half (so a leaf still has room to grow its own subtree); we additionally
        //    cap that span at `CHILD_RESERVE` so a wide fan does not exhaust a large
        //    parent interval after only `log2(width)` children (see `CHILD_RESERVE`).
        let (_p_start, p_end) = self.intervals[&selected_parent];
        let rem_lo = self.next_free[&selected_parent];
        if rem_lo <= p_end {
            // First half of the remaining span, but never more than `CHILD_RESERVE`
            // points. `capped` cannot overflow `p_end`: the half-split is already
            // `<= p_end`, and the `.min` only lowers the endpoint.
            let half = rem_lo + (p_end - rem_lo) / 2;
            let capped = rem_lo.saturating_add(CHILD_RESERVE - 1);
            let end = half.min(capped);
            self.intervals.insert(id, (rem_lo, end));
            self.next_free.insert(id, rem_lo + 1);
            self.next_free.insert(selected_parent, end + 1);
        } else {
            // Parent is out of capacity: place a temporary interval, then reindex
            // the minimal enclosing subtree to make room (interval reindexing).
            self.intervals.insert(id, (rem_lo, rem_lo));
            self.next_free.insert(id, rem_lo);
            self.reindex(selected_parent);
        }

        // 3. Insert `id` into the future-covering set of each merged block
        //    (Kaspa's `insert_to_future_covering_set`).
        for merged in mergeset {
            self.insert_to_future_covering_set(*merged, id);
        }
    }

    /// Reindex (re-lay-out) the smallest subtree that can regain interval
    /// capacity for `node`: the lowest ancestor whose interval is at least twice
    /// its subtree size (the root always qualifies, as its capacity dwarfs any
    /// realistic block count). Only that subtree's intervals change; the
    /// ancestor's own `[start, end]` is preserved, so nothing outside it moves.
    fn reindex(&mut self, node: BlockId) {
        let mut anchor = node;
        loop {
            let (lo, hi) = self.intervals[&anchor];
            let capacity = hi - lo + 1;
            let need = self.subtree_size[&anchor];
            // Enough room (with 2x slack) — or we've reached the root, which is
            // always large enough — stop here. The root fallback assumes the DAG
            // holds fewer than `ROOT_CAPACITY` (~9.2e18) blocks; beyond that the
            // root interval could no longer cover its subtree (not reachable in
            // practice, and the interval space would need widening first).
            if capacity >= need.saturating_mul(2) || !self.tree_parent.contains_key(&anchor) {
                self.reindexes += 1;
                self.relayout_touches += need;
                self.relayout_subtree(anchor, lo, hi);
                return;
            }
            anchor = self.tree_parent[&anchor];
        }
    }

    /// Re-lay-out the tree subtree rooted at `root` within the interval
    /// `[lo, hi]`, sizing each child's sub-interval proportionally to its subtree
    /// size (with leftover slack distributed proportionally too). Iterative to
    /// tolerate deep chains. Precondition: `hi - lo + 1 >= subtree_size(root)`.
    fn relayout_subtree(&mut self, root: BlockId, lo: u64, hi: u64) {
        let mut stack = vec![(root, lo, hi)];
        while let Some((node, lo, hi)) = stack.pop() {
            self.intervals.insert(node, (lo, hi));
            let total = self.subtree_size[&node] - 1; // descendants = points needed
            if total == 0 {
                // Leaf: whole child region `[lo+1, hi]` is free capacity.
                self.next_free.insert(node, lo + 1);
                continue;
            }
            let region = hi - lo; // points available for children: [lo+1, hi]
            let slack = region - total; // >= 0 by the precondition / recursion
            let children = self.tree_children[&node].clone();
            let mut cursor = lo + 1;
            for child in children {
                let sz = self.subtree_size[&child];
                // Proportional slice: its own subtree plus a proportional share of
                // the slack. Sum over children stays <= region, so every child
                // ends within `[lo+1, hi]` and each gets at least `sz` points.
                let extra = ((slack as u128) * (sz as u128) / (total as u128)) as u64;
                // Cap the slice at `CHILD_RESERVE` (but never below the child's own
                // subtree size) for the same amortisation reason as incremental
                // allocation: a re-laid-out wide fan must not hand each leaf an
                // exponential share of the interval, or the parent's reserve is
                // consumed and it re-exhausts at once. Capping only *lowers* a span,
                // so the sum still fits and each child still gets at least `sz`
                // points — the precondition and containment structure are preserved,
                // and the trailing space stays as this node's reserve.
                let cap = CHILD_RESERVE.max(sz);
                let span = (sz + extra).min(cap);
                let c_lo = cursor;
                let c_hi = cursor + (span - 1);
                stack.push((child, c_lo, c_hi));
                cursor = c_hi + 1;
            }
            // Whatever is left after the last child stays as this node's free
            // capacity for future children.
            self.next_free.insert(node, cursor);
        }
    }

    /// Insert `b` into `a`'s future-covering set, keeping it minimal and sorted by
    /// interval start (Kaspa's `insert_to_future_covering_set`). `b` covers a
    /// block `x` iff `b` is a tree-ancestor of `x` (its interval contains `x`'s).
    fn insert_to_future_covering_set(&mut self, a: BlockId, b: BlockId) {
        let (b_start, b_end) = self.intervals[&b];
        // Split borrows so the covering closure can read `intervals` while we
        // mutate the fcs vector.
        let Self { intervals, fcs, .. } = self;
        let set = fcs.entry(a).or_default();
        // Position: first entry whose start exceeds b's start.
        let pos = set.partition_point(|c| intervals[c].0 <= b_start);
        // If the immediate predecessor already covers `b` (is a tree-ancestor of
        // it), `b` is redundant — leave the set unchanged.
        if pos > 0 {
            let (ps, pe) = intervals[&set[pos - 1]];
            if ps <= b_start && b_end <= pe {
                return;
            }
        }
        // Drop any following entries that `b` now covers (b a tree-ancestor of
        // them), so the set stays minimal.
        while pos < set.len() {
            let (cs, ce) = intervals[&set[pos]];
            if b_start <= cs && ce <= b_end {
                set.remove(pos);
            } else {
                break;
            }
        }
        set.insert(pos, b);
    }

    /// Amortisation metrics `(reindexes, relayout_touches)`: how many interval
    /// reindexes this oracle has performed and the total number of tree nodes those
    /// reindexes re-laid-out. Both are pure bookkeeping — they never affect a query
    /// answer — exposed so tests can prove the interval-allocation tuning keeps
    /// reindex cost low. `relayout_touches` is the headline amortised-cost figure.
    pub fn reindex_metrics(&self) -> (u64, u64) {
        (self.reindexes, self.relayout_touches)
    }

    /// Whether `x` is a tree- (selected-chain-) ancestor of, or equal to, `y`.
    fn tree_reaches(&self, x: &BlockId, y: &BlockId) -> bool {
        match (self.intervals.get(x), self.intervals.get(y)) {
            (Some(&(xs, xe)), Some(&(ys, _))) => xs <= ys && ys <= xe,
            _ => false,
        }
    }

    /// Whether `ancestor` is a strict tree- (selected-chain-) ancestor of
    /// `descendant`.
    pub fn is_chain_ancestor(&self, ancestor: &BlockId, descendant: &BlockId) -> bool {
        ancestor != descendant && self.tree_reaches(ancestor, descendant)
    }

    /// Whether `ancestor` is a strict DAG-ancestor of `descendant` (i.e.
    /// `ancestor ∈ past(descendant)`). `false` for equal ids.
    pub fn is_ancestor(&self, ancestor: &BlockId, descendant: &BlockId) -> bool {
        if ancestor == descendant {
            return false;
        }
        if self.tree_reaches(ancestor, descendant) {
            return true; // in the tree subtree
        }
        // Otherwise `descendant` must sit under one of `ancestor`'s future-covering
        // blocks' tree subtrees.
        self.fcs
            .get(ancestor)
            .is_some_and(|covers| covers.iter().any(|c| self.tree_reaches(c, descendant)))
    }
}

/// Post-order tree-subtree sizes (each node counts itself). Iterative to tolerate
/// deep chains.
fn subtree_sizes(
    root: BlockId,
    tree_children: &HashMap<BlockId, Vec<BlockId>>,
) -> HashMap<BlockId, u64> {
    let mut sizes: HashMap<BlockId, u64> = HashMap::new();

    enum Step {
        Enter(BlockId),
        Exit(BlockId),
    }
    let mut stack = vec![Step::Enter(root)];
    while let Some(step) = stack.pop() {
        match step {
            Step::Enter(id) => {
                sizes.insert(id, 1);
                stack.push(Step::Exit(id));
                if let Some(children) = tree_children.get(&id) {
                    for child in children {
                        stack.push(Step::Enter(*child));
                    }
                }
            }
            Step::Exit(id) => {
                if let Some(children) = tree_children.get(&id) {
                    // A node's size is 1 plus the sizes of its children's subtrees,
                    // all finalised by now (post-order).
                    let child_total: u64 = children.iter().map(|c| sizes[c]).sum();
                    sizes.insert(id, 1 + child_total);
                }
            }
        }
    }
    sizes
}

/// For each block, the tree-roots of `future(block) \ subtree(block)`.
fn build_fcs(
    dag: &Dag,
    dag_children: &HashMap<BlockId, Vec<BlockId>>,
    intervals: &HashMap<BlockId, (u64, u64)>,
) -> HashMap<BlockId, Vec<BlockId>> {
    let subtree_contains = |x: &BlockId, y: &BlockId| -> bool {
        match (intervals.get(x), intervals.get(y)) {
            (Some(&(xs, xe)), Some(&(ys, _))) => xs <= ys && ys <= xe,
            _ => false,
        }
    };

    let mut fcs: HashMap<BlockId, Vec<BlockId>> = HashMap::new();
    for a in dag.nodes.keys() {
        // future(a) = everything reachable forward over DAG child edges, minus a.
        let mut future: HashSet<BlockId> = HashSet::new();
        let mut queue: VecDeque<BlockId> = VecDeque::new();
        queue.push_back(*a);
        while let Some(cur) = queue.pop_front() {
            if let Some(children) = dag_children.get(&cur) {
                for child in children {
                    if future.insert(*child) {
                        queue.push_back(*child);
                    }
                }
            }
        }

        // candidate = future(a) outside a's tree subtree. Its tree-roots (members
        // whose selected parent is not itself a candidate) are the covering set.
        // The candidate set is closed under tree-descendants, so this is exact.
        let mut covers: Vec<BlockId> = future
            .iter()
            .copied()
            .filter(|c| !subtree_contains(a, c))
            .filter(|c| {
                let tree_parent = dag.nodes[c].ghostdag.selected_parent;
                match tree_parent {
                    // Root of the candidate set iff its tree parent is not also a
                    // candidate (either in a's subtree, or not in a's future).
                    Some(tp) => subtree_contains(a, &tp) || !future.contains(&tp),
                    None => true,
                }
            })
            .collect();
        covers.sort_unstable_by_key(|c| intervals.get(c).map_or(0, |iv| iv.0));
        if !covers.is_empty() {
            fcs.insert(*a, covers);
        }
    }
    fcs
}
