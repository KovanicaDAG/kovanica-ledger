//! Integration tests for finality-depth pruning and re-orgs.
//!
//! A [`Ledger::with_finality`] ledger prunes the stored state of blocks more than
//! `finality_depth` blue score below the selected tip, rejects blocks that build
//! on that final history, and still reports the correct current state (which
//! follows the selected tip, so re-orgs above the finality point are implicit).

use kovanica_state::{
    apply_dag, HalvingSchedule, KeyPair, Ledger, LedgerInsertError, OutPoint, Transaction,
    TxOutput, DEFAULT_HALVING_ERA,
};

const K: u16 = 3;
const SUBSIDY: u64 = 1_000;
const SCHEDULE: HalvingSchedule = HalvingSchedule::new(SUBSIDY, DEFAULT_HALVING_ERA);

/// A ledger with a genesis coinbase minting `funding` to actor 1; returns the
/// ledger and the coinbase outpoint.
fn funded(finality_depth: u64, funding: u64) -> (Ledger, OutPoint) {
    let coinbase = Transaction::coinbase(
        vec![TxOutput::new(funding, KeyPair::from_u64(1).address())],
        b"genesis".to_vec(),
    );
    let coin = OutPoint::new(coinbase.id(), 0);
    let ledger = Ledger::with_finality(K, SCHEDULE, &[coinbase], finality_depth).unwrap();
    (ledger, coin)
}

#[test]
fn old_block_states_are_pruned_below_the_finality_point() {
    // A long linear chain of empty blocks with finality depth 3: once the tip is
    // far enough ahead, early blocks are final and their state is dropped, while
    // the recent ones (and the tip) are kept.
    let (mut ledger, _coin) = funded(3, 500);
    let mut ids = vec![ledger.genesis()];
    for _ in 0..10 {
        let parent = *ids.last().unwrap();
        ids.push(ledger.insert(vec![parent], 1, 0, 0, &[]).unwrap());
    }

    let tip = ledger.dag().selected_tip();
    assert!(ledger.state(&tip).is_some(), "tip state kept");
    assert!(ledger.state(&ledger.genesis()).is_none(), "genesis pruned");

    // A state is retained iff the block is at or above the finality score.
    let threshold = ledger.finality_score();
    assert!(threshold > 0, "finality has kicked in");
    for id in &ids {
        let score = ledger.dag().ghostdag(id).unwrap().blue_score;
        assert_eq!(
            ledger.state(id).is_some(),
            score >= threshold,
            "block at score {score} vs threshold {threshold}"
        );
    }

    // Pruning does not corrupt the current state (empty blocks move nothing), and
    // it still matches a from-scratch batch apply.
    assert_eq!(ledger.ledger_state().total_value(), 500);
    assert_eq!(
        ledger.ledger_state().total_value(),
        apply_dag(ledger.dag(), SUBSIDY).utxo.total_value()
    );
}

#[test]
fn building_on_final_history_is_rejected() {
    // Extend a chain well past the finality depth, then try to fork from an early
    // (now final) block. The insert is a finality violation and adds nothing.
    let (mut ledger, coin) = funded(2, 500);
    let genesis = ledger.genesis();
    let early = ledger.insert(vec![genesis], 1, 0, 0, &[]).unwrap();
    let mut tip = early;
    for _ in 0..8 {
        tip = ledger.insert(vec![tip], 1, 0, 0, &[]).unwrap();
    }
    let early_score = ledger.dag().ghostdag(&early).unwrap().blue_score;
    assert!(ledger.finality_score() > early_score, "early is now final");

    let before = ledger.dag().len();
    // A valid spend, but built on final history → rejected on finality.
    let alice = KeyPair::from_u64(1);
    let tx = Transaction::signed(
        &[(coin, &alice)],
        vec![TxOutput::new(500, KeyPair::from_u64(2).address())],
        vec![],
    );
    let err = ledger.insert(vec![early], 1, 0, 0, &[tx]).unwrap_err();
    assert!(
        matches!(err, LedgerInsertError::Finality { .. }),
        "got {err:?}"
    );
    assert_eq!(ledger.dag().len(), before, "no rejected block was added");

    // Building on the tip (non-final) still works.
    assert!(ledger.insert(vec![tip], 1, 0, 0, &[]).is_ok());
}

#[test]
fn unbounded_ledger_never_prunes() {
    let (mut ledger, _coin) = funded(u64::MAX, 500);
    let mut tip = ledger.genesis();
    for _ in 0..10 {
        tip = ledger.insert(vec![tip], 1, 0, 0, &[]).unwrap();
    }
    assert_eq!(ledger.finality_score(), 0);
    for id in ledger.dag().linearize() {
        assert!(ledger.state(&id).is_some(), "nothing pruned when unbounded");
    }
}

#[test]
fn reorg_above_finality_follows_the_heavier_branch() {
    // Two branches off genesis; the heavier one (more work) becomes the selected
    // tip and the current state follows it — an implicit re-org, no revert.
    let (mut ledger, coin) = funded(u64::MAX, 500);
    let genesis = ledger.genesis();
    let alice = KeyPair::from_u64(1);
    let bob = KeyPair::from_u64(2);
    let carol = KeyPair::from_u64(3);

    // Light branch: alice -> bob (block `a`), the only tip, so it's selected.
    let to_bob = Transaction::signed(
        &[(coin, &alice)],
        vec![TxOutput::new(500, bob.address())],
        b"bob".to_vec(),
    );
    let a = ledger.insert(vec![genesis], 1, 0, 0, &[to_bob]).unwrap();
    assert_eq!(ledger.dag().selected_tip(), a);
    assert_eq!(ledger.ledger_state().balance(&bob.address()), 500);

    // Heavy branch off genesis: a high-work block `h1` (alice -> carol) that a
    // descendant `h2` builds on, so h2 accumulates that work and overtakes `a`.
    // (A block's own work counts toward its *descendants'* blue work, not its own.)
    let to_carol = Transaction::signed(
        &[(coin, &alice)],
        vec![TxOutput::new(500, carol.address())],
        b"carol".to_vec(),
    );
    let h1 = ledger
        .insert(vec![genesis], 100, 0, 0, &[to_carol])
        .unwrap();
    let h2 = ledger.insert(vec![h1], 1, 0, 0, &[]).unwrap();

    assert_eq!(ledger.dag().selected_tip(), h2, "heavier branch selected");
    // The current state now reflects the heavy branch; the light spend lost.
    assert_eq!(ledger.ledger_state().balance(&carol.address()), 500);
    assert_eq!(ledger.ledger_state().balance(&bob.address()), 0);
}
