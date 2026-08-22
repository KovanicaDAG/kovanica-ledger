//! Differential tests for the reachability oracle (now the DAG's backing for
//! `is_ancestor`): on many DAGs — structured and randomly generated adversarial
//! ones — the oracle must agree with an **independent** ground truth (a naive
//! backward walk over parent edges) for *every* ordered pair of blocks, and its
//! chain-ancestor answer must match walking selected parents.

use std::collections::{HashSet, VecDeque};

use kovanica_dag::{Block, BlockId, Dag, Reachability};

/// A tiny deterministic PRNG (SplitMix-ish LCG) so tests are reproducible.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0
    }

    fn below(&mut self, n: usize) -> usize {
        ((self.next() >> 33) as usize) % n.max(1)
    }
}

/// Build a random DAG of `n` non-genesis blocks with parameter `k`. Each block
/// references 1–3 distinct existing blocks and has a small random work, producing
/// varied shapes (chains, wide forks, deep merges).
fn random_dag(seed: u64, n: usize, k: u16) -> (Dag, Vec<BlockId>) {
    let genesis = Block::genesis(1, 0, 0, b"genesis".to_vec());
    let genesis_id = genesis.id();
    let mut dag = Dag::new(k, genesis);
    let mut ids = vec![genesis_id];
    let mut rng = Rng(seed.wrapping_add(0x9E3779B97F4A7C15));

    for i in 0..n {
        let want = 1 + rng.below(3);
        let mut parents: Vec<BlockId> = Vec::new();
        for _ in 0..(want * 4) {
            if parents.len() == want {
                break;
            }
            let cand = ids[rng.below(ids.len())];
            if !parents.contains(&cand) {
                parents.push(cand);
            }
        }
        let work = 1 + rng.below(4) as u128;
        let id = dag
            .insert(Block::new(
                parents,
                work,
                0,
                0,
                format!("b{i}").into_bytes(),
            ))
            .expect("random block is valid");
        ids.push(id);
    }
    (dag, ids)
}

/// Independent ground truth: is `a` a strict DAG-ancestor of `b`? Naive backward
/// BFS over parent edges (this is what the oracle must reproduce).
fn naive_is_ancestor(dag: &Dag, a: &BlockId, b: &BlockId) -> bool {
    if a == b {
        return false;
    }
    let mut seen: HashSet<BlockId> = HashSet::new();
    let mut queue: VecDeque<BlockId> = dag.block(b).unwrap().parents().iter().copied().collect();
    while let Some(x) = queue.pop_front() {
        if x == *a {
            return true;
        }
        if !seen.insert(x) {
            continue;
        }
        for parent in dag.block(&x).unwrap().parents() {
            queue.push_back(*parent);
        }
    }
    false
}

/// Reference: is `a` a strict selected-parent (chain) ancestor of `b`?
fn walk_chain_ancestor(dag: &Dag, a: &BlockId, b: &BlockId) -> bool {
    if a == b {
        return false;
    }
    let mut cur = dag.ghostdag(b).and_then(|g| g.selected_parent);
    while let Some(c) = cur {
        if c == *a {
            return true;
        }
        cur = dag.ghostdag(&c).and_then(|g| g.selected_parent);
    }
    false
}

/// Assert the oracle (via both `Dag::is_ancestor` and a freshly-built
/// `Reachability`) agrees with the naive ground truth on every ordered pair.
fn assert_oracle_matches(dag: &Dag, ids: &[BlockId]) {
    let oracle = Reachability::build(dag);
    for a in ids {
        for b in ids {
            let truth = naive_is_ancestor(dag, a, b);
            assert_eq!(dag.is_ancestor(a, b), truth, "Dag::is_ancestor ({a}, {b})");
            assert_eq!(
                oracle.is_ancestor(a, b),
                truth,
                "oracle.is_ancestor ({a}, {b})"
            );
            assert_eq!(
                oracle.is_chain_ancestor(a, b),
                walk_chain_ancestor(dag, a, b),
                "is_chain_ancestor mismatch for ({a}, {b})"
            );
        }
    }
}

#[test]
fn matches_past_sets_on_random_dags() {
    // A spread of seeds, sizes and k values, including k = 0 (aggressive reds).
    for seed in 0..40u64 {
        let k = (seed % 4) as u16;
        let n = 12 + (seed as usize % 25);
        let (dag, ids) = random_dag(seed, n, k);
        assert_oracle_matches(&dag, &ids);
    }
}

/// The core behaviour-preservation guard for the *incremental* oracle: the
/// answers `Dag::is_ancestor`/`is_chain_ancestor` give (maintained incrementally
/// by `Reachability::add_block` on each insert) must equal, for **every** ordered
/// pair, the answers a from-scratch `Reachability::build` of the same DAG gives —
/// checked after *each* insert, not only at the end. This pins the incremental
/// path to the from-scratch construction directly, independently of the naive
/// ground truth.
fn assert_incremental_equals_fresh(dag: &Dag, ids: &[BlockId]) {
    let fresh = Reachability::build(dag);
    for a in ids {
        for b in ids {
            // `Dag::is_ancestor` reads the incrementally-maintained oracle; `fresh`
            // is the from-scratch construction. The two must agree on every ordered
            // pair. `is_ancestor` exercises both the tree-interval query and the
            // future-covering-set query, and `is_chain_ancestor` shares the same
            // `tree_reaches` path, so this O(1)-per-pair check covers the whole
            // query surface — cheap enough to run after every single insert. Full
            // `is_chain_ancestor`-vs-walk coverage is done once via
            // `assert_oracle_matches` at the end of each scenario.
            assert_eq!(
                dag.is_ancestor(a, b),
                fresh.is_ancestor(a, b),
                "incremental vs fresh is_ancestor ({a}, {b})"
            );
        }
    }
}

/// Build random adversarial DAGs and, after *every* insert, assert the
/// incrementally-maintained oracle answers identically to a freshly-built one on
/// all ordered pairs. Larger `n` than the differential test, so more of the
/// interval space (and reindexing) is exercised.
#[test]
fn incremental_matches_fresh_after_every_insert() {
    for seed in 0..24u64 {
        let k = (seed % 5) as u16;
        let n = 40 + (seed as usize % 40);

        let genesis = Block::genesis(1, 0, 0, b"genesis".to_vec());
        let genesis_id = genesis.id();
        let mut dag = Dag::new(k, genesis);
        let mut ids = vec![genesis_id];
        let mut rng = Rng(seed.wrapping_add(0xD1B54A32D192ED03));

        for i in 0..n {
            let want = 1 + rng.below(3);
            let mut parents: Vec<BlockId> = Vec::new();
            for _ in 0..(want * 4) {
                if parents.len() == want {
                    break;
                }
                let cand = ids[rng.below(ids.len())];
                if !parents.contains(&cand) {
                    parents.push(cand);
                }
            }
            let work = 1 + rng.below(4) as u128;
            let id = dag
                .insert(Block::new(
                    parents,
                    work,
                    0,
                    0,
                    format!("b{i}").into_bytes(),
                ))
                .expect("random block is valid");
            ids.push(id);
            // Behaviour preservation must hold at every step, not just the end.
            assert_incremental_equals_fresh(&dag, &ids);
        }
        // End-to-end: also pin the final DAG to the naive ground truth and the
        // selected-parent-walk chain-ancestor reference.
        assert_oracle_matches(&dag, &ids);
    }
}

/// A long linear chain forces the tree interval capacity to shrink (each child
/// takes half the parent's remaining span) until it is exhausted and a reindex
/// reallocates the branch. Hundreds of blocks deep guarantees many reindexes.
#[test]
fn incremental_survives_a_long_chain_with_reindexing() {
    let genesis = Block::genesis(1, 0, 0, b"genesis".to_vec());
    let mut dag = Dag::new(3, genesis);
    let mut ids = vec![dag.genesis()];
    for i in 0..600 {
        let parent = *ids.last().unwrap();
        ids.push(
            dag.insert(Block::new(
                vec![parent],
                1,
                0,
                0,
                format!("c{i}").into_bytes(),
            ))
            .unwrap(),
        );
    }
    // Only the O(n^2) incremental-vs-fresh check here (naive ground truth, an
    // extra O(n) per pair, is established at moderate n by the random tests).
    assert_incremental_equals_fresh(&dag, &ids);
}

/// A very wide fan: one parent with hundreds of direct tree children. Each child
/// consumes a halving of the parent's remaining capacity, so the parent's
/// interval is exhausted repeatedly and reindexing must grow it. Then a single
/// block merges every child, exercising a large mergeset / future-covering set.
#[test]
fn incremental_survives_a_wide_fan_with_reindexing() {
    let genesis = Block::genesis(1, 0, 0, b"genesis".to_vec());
    let mut dag = Dag::new(400, genesis); // k large so the fan stays all-blue
    let g = dag.genesis();
    let mut ids = vec![g];
    let mut children: Vec<BlockId> = Vec::new();
    for i in 0..400 {
        let id = dag
            .insert(Block::new(vec![g], 1, 0, 0, format!("w{i}").into_bytes()))
            .unwrap();
        children.push(id);
        ids.push(id);
    }
    let merge = dag
        .insert(Block::new(children, 1, 0, 0, b"merge".to_vec()))
        .unwrap();
    ids.push(merge);
    assert_incremental_equals_fresh(&dag, &ids);
}

/// A deep-and-wide mix: a spine of blocks, each fanning out a handful of side
/// children that later merge back. Combines chain-depth reindexing with
/// non-trivial future-covering sets.
#[test]
fn incremental_survives_deep_and_wide_mix() {
    let genesis = Block::genesis(1, 0, 0, b"genesis".to_vec());
    let mut dag = Dag::new(8, genesis);
    let mut ids = vec![dag.genesis()];
    let mut spine = dag.genesis();
    for level in 0..120 {
        // Fan out several side blocks off the current spine tip …
        let mut side: Vec<BlockId> = Vec::new();
        for s in 0..4 {
            let id = dag
                .insert(Block::new(
                    vec![spine],
                    1,
                    0,
                    0,
                    format!("l{level}s{s}").into_bytes(),
                ))
                .unwrap();
            side.push(id);
            ids.push(id);
        }
        // … then merge them all into the next spine block.
        spine = dag
            .insert(Block::new(
                side,
                1,
                0,
                0,
                format!("l{level}spine").into_bytes(),
            ))
            .unwrap();
        ids.push(spine);
    }
    assert_incremental_equals_fresh(&dag, &ids);
}

/// Larger random adversarial DAGs (bigger `n` than the differential test), each
/// validated end-to-end against both the naive ground truth (via
/// `assert_oracle_matches`) and a fresh oracle (via `assert_incremental_equals_fresh`).
#[test]
fn incremental_matches_on_larger_random_dags() {
    for seed in 0..12u64 {
        let k = (seed % 4) as u16;
        let n = 90 + (seed as usize % 40);
        let (dag, ids) = random_dag(seed.wrapping_mul(2654435761), n, k);
        assert_oracle_matches(&dag, &ids);
        assert_incremental_equals_fresh(&dag, &ids);
    }
}

#[test]
fn matches_on_a_linear_chain() {
    let genesis = Block::genesis(1, 0, 0, b"genesis".to_vec());
    let mut dag = Dag::new(3, genesis);
    let mut ids = vec![dag.genesis()];
    for i in 0..20 {
        let parent = *ids.last().unwrap();
        ids.push(
            dag.insert(Block::new(
                vec![parent],
                1,
                0,
                0,
                format!("c{i}").into_bytes(),
            ))
            .unwrap(),
        );
    }
    assert_oracle_matches(&dag, &ids);
}

#[test]
fn matches_on_a_wide_fork_and_merge() {
    let genesis = Block::genesis(1, 0, 0, b"genesis".to_vec());
    let mut dag = Dag::new(2, genesis);
    let g = dag.genesis();
    let parallel: Vec<BlockId> = (0..8)
        .map(|i| {
            dag.insert(Block::new(vec![g], 1, 0, 0, format!("w{i}").into_bytes()))
                .unwrap()
        })
        .collect();
    let merge = dag
        .insert(Block::new(parallel.clone(), 1, 0, 0, b"m".to_vec()))
        .unwrap();

    let mut ids = vec![g];
    ids.extend(parallel);
    ids.push(merge);
    assert_oracle_matches(&dag, &ids);
}

#[test]
fn matches_on_a_diamond() {
    let genesis = Block::genesis(1, 0, 0, b"genesis".to_vec());
    let mut dag = Dag::new(3, genesis);
    let g = dag.genesis();
    let a = dag
        .insert(Block::new(vec![g], 1, 0, 0, b"a".to_vec()))
        .unwrap();
    let b = dag
        .insert(Block::new(vec![g], 1, 0, 0, b"b".to_vec()))
        .unwrap();
    let m = dag
        .insert(Block::new(vec![a, b], 1, 0, 0, b"m".to_vec()))
        .unwrap();
    // A tail block off only one side, to exercise a non-tree covering path.
    let c = dag
        .insert(Block::new(vec![a], 1, 0, 0, b"c".to_vec()))
        .unwrap();
    assert_oracle_matches(&dag, &[g, a, b, m, c]);
}

// ---------------------------------------------------------------------------
// Interval-reindex amortisation tuning (Stage 2)
//
// A wide fan under one selected parent is the pathological input for interval
// allocation: before the `CHILD_RESERVE` cap, every leaf halved the parent's
// remaining interval, so the parent exhausted its capacity every ~log2(width)
// children and each further child forced a reindex that re-laid-out the *whole*
// fan — `O(width^2)` reindex work. The cap bounds each child's slice, so a fan
// far below `interval_width / CHILD_RESERVE` (~8M off genesis) is reindex-free.
//
// These tests pin BOTH halves of the change: correctness parity against the
// naive brute-force oracle (the tuning only renumbers intervals, it must not
// change a single answer) and the amortisation metric (reindexes actually
// dropped). `Dag::reachability_reindex_metrics()` exposes `(reindexes,
// relayout_touches)` of the incremental oracle — pure bookkeeping.
// ---------------------------------------------------------------------------

/// A moderate wide fan + a merge, checked exhaustively against the naive
/// brute-force oracle (every ordered pair) *and* a freshly-built oracle, plus the
/// amortisation metric: this pattern used to reindex repeatedly; it now performs
/// zero reindexes.
#[test]
fn wide_fan_reindex_is_amortised_away() {
    let genesis = Block::genesis(1, 0, 0, b"genesis".to_vec());
    let mut dag = Dag::new(u16::MAX, genesis); // k huge so the whole fan stays blue
    let g = dag.genesis();
    let mut ids = vec![g];
    let mut children: Vec<BlockId> = Vec::new();
    for i in 0..250 {
        let id = dag
            .insert(Block::new(vec![g], 1, 0, 0, format!("w{i}").into_bytes()))
            .unwrap();
        children.push(id);
        ids.push(id);
    }
    let merge = dag
        .insert(Block::new(children, 1, 0, 0, b"merge".to_vec()))
        .unwrap();
    ids.push(merge);

    // Behaviour preserved: matches the naive brute-force oracle AND a fresh build
    // on every ordered pair.
    assert_oracle_matches(&dag, &ids);
    assert_incremental_equals_fresh(&dag, &ids);

    // Amortisation: this fan (251 « ~8M) triggers no reindex at all.
    let (reindexes, touches) = dag.reachability_reindex_metrics();
    assert_eq!(reindexes, 0, "wide fan must be reindex-free after tuning");
    assert_eq!(touches, 0, "a reindex-free fan re-lays-out no nodes");
}

/// A large wide fan (thousands of siblings) — the size at which the old halving
/// allocation did its worst `O(width^2)` reindex work. Correctness is pinned to a
/// freshly-built oracle on every pair (the O(width) naive walk is too slow at this
/// size and is covered by the moderate test above), and the amortisation metric
/// asserts the whole fan is still reindex-free.
#[test]
fn large_wide_fan_stays_reindex_free() {
    let genesis = Block::genesis(1, 0, 0, b"genesis".to_vec());
    let mut dag = Dag::new(u16::MAX, genesis);
    let g = dag.genesis();
    let mut ids = vec![g];
    let mut children: Vec<BlockId> = Vec::new();
    for i in 0..2000 {
        let id = dag
            .insert(Block::new(vec![g], 1, 0, 0, format!("w{i}").into_bytes()))
            .unwrap();
        children.push(id);
        ids.push(id);
    }
    let merge = dag
        .insert(Block::new(children, 1, 0, 0, b"merge".to_vec()))
        .unwrap();
    ids.push(merge);

    assert_incremental_equals_fresh(&dag, &ids);
    let (reindexes, touches) = dag.reachability_reindex_metrics();
    // Before the tuning this fan reindexed hundreds of times, re-laying-out
    // hundreds of thousands of nodes; now it does none.
    assert_eq!(
        reindexes, 0,
        "a 2000-wide fan must be reindex-free after tuning"
    );
    assert_eq!(touches, 0, "a reindex-free fan re-lays-out no nodes");
}

/// Adversarial worst case for the reindex path itself: a long linear chain. Each
/// block's interval still halves toward its parent's tip, so the tip churns and
/// reindexes fire repeatedly — the case where reindexing is *legitimately*
/// exercised. This guards that the amortisation tuning did not change any answer
/// where the reindex path genuinely runs: every pair still matches a freshly-built
/// oracle after the whole chain is built.
#[test]
fn deep_chain_still_reindexes_and_stays_correct() {
    let genesis = Block::genesis(1, 0, 0, b"genesis".to_vec());
    let mut dag = Dag::new(3, genesis);
    let mut ids = vec![dag.genesis()];
    for i in 0..800 {
        let parent = *ids.last().unwrap();
        ids.push(
            dag.insert(Block::new(
                vec![parent],
                1,
                0,
                0,
                format!("c{i}").into_bytes(),
            ))
            .unwrap(),
        );
    }
    // The reindex path is genuinely exercised here (a chain still churns its tip).
    let (reindexes, _) = dag.reachability_reindex_metrics();
    assert!(reindexes > 0, "a long chain must still drive reindexes");
    // ...and every answer still matches a from-scratch oracle. (Full naive
    // brute-force parity for chains is covered at moderate n by
    // `matches_on_a_linear_chain` and the random-DAG tests; the O(n^2)
    // incremental-vs-fresh check here is the behaviour-preservation guard.)
    assert_incremental_equals_fresh(&dag, &ids);
}
