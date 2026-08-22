//! Integration tests for ledger persistence: a [`Ledger`] round-trips through a
//! snapshot by replaying its blocks, so the restored ledger — its DAG, its full
//! state, and every block's view state — is identical to the original.

use kovanica_state::{
    Address, HalvingSchedule, KeyPair, Ledger, LedgerSnapshotError, OutPoint, Transaction,
    TxOutput, UtxoSet, DEFAULT_HALVING_ERA,
};

const K: u16 = 3;
const SUBSIDY: u64 = 1_000;
const SCHEDULE: HalvingSchedule = HalvingSchedule::new(SUBSIDY, DEFAULT_HALVING_ERA);

fn snapshot(utxo: &UtxoSet) -> Vec<(OutPoint, u64, Address)> {
    let mut rows: Vec<(OutPoint, u64, Address)> =
        utxo.iter().map(|(op, o)| (*op, o.value, o.owner)).collect();
    rows.sort_by_key(|row| row.0);
    rows
}

/// Build a non-trivial ledger: a chain, a parallel side block, and a merge.
fn build_ledger() -> Ledger {
    let alice = KeyPair::from_u64(1);
    let bob = KeyPair::from_u64(2);
    let carol = KeyPair::from_u64(3);

    let coinbase = Transaction::coinbase(
        vec![TxOutput::new(500, alice.address())],
        b"genesis".to_vec(),
    );
    let coin = OutPoint::new(coinbase.id(), 0);
    let mut ledger = Ledger::new(K, SCHEDULE, &[coinbase]).unwrap();
    let genesis = ledger.genesis();

    // alice → bob (300) with 200 change back to alice.
    let a_to_b = Transaction::signed(
        &[(coin, &alice)],
        vec![
            TxOutput::new(300, bob.address()),
            TxOutput::new(200, alice.address()),
        ],
        Vec::new(),
    );
    let bob_coin = OutPoint::new(a_to_b.id(), 0);
    let alice_change = OutPoint::new(a_to_b.id(), 1);
    let b1 = ledger.insert(vec![genesis], 1, 0, 0, &[a_to_b]).unwrap();

    // bob → carol (300) on b1.
    let b_to_c = Transaction::signed(
        &[(bob_coin, &bob)],
        vec![TxOutput::new(300, carol.address())],
        Vec::new(),
    );
    let b2 = ledger.insert(vec![b1], 1, 0, 0, &[b_to_c]).unwrap();

    // Parallel side block off b1: alice spends her change to carol.
    let side = Transaction::signed(
        &[(alice_change, &alice)],
        vec![TxOutput::new(200, carol.address())],
        b"side".to_vec(),
    );
    let side_block = ledger.insert(vec![b1], 1, 0, 0, &[side]).unwrap();

    // Merge the two tips (heavier work to make the merge the selected tip).
    ledger.insert(vec![b2, side_block], 5, 0, 0, &[]).unwrap();
    ledger
}

#[test]
fn ledger_roundtrips_through_a_snapshot() {
    let ledger = build_ledger();
    let bytes = ledger.write_snapshot();
    let restored = Ledger::read_snapshot(&bytes).expect("snapshot decodes");

    // Same DAG shape and order.
    assert_eq!(restored.dag().linearize(), ledger.dag().linearize());
    assert_eq!(restored.dag().tips(), ledger.dag().tips());
    assert_eq!(restored.genesis(), ledger.genesis());
    assert_eq!(restored.subsidy(), ledger.subsidy());

    // Same full ledger state.
    assert_eq!(
        snapshot(&restored.ledger_state()),
        snapshot(&ledger.ledger_state())
    );

    // Same per-block view state for every block.
    for id in ledger.dag().linearize() {
        assert_eq!(
            snapshot(restored.state(&id).unwrap()),
            snapshot(ledger.state(&id).unwrap()),
            "per-block state differs for {id}"
        );
    }
}

#[test]
fn snapshot_is_stable_across_a_second_roundtrip() {
    // Re-serialising a restored ledger yields identical bytes — the format is
    // canonical (blocks in the deterministic linearized order).
    let ledger = build_ledger();
    let bytes1 = ledger.write_snapshot();
    let restored = Ledger::read_snapshot(&bytes1).unwrap();
    let bytes2 = restored.write_snapshot();
    assert_eq!(bytes1, bytes2);
}

#[test]
fn bad_magic_is_rejected() {
    assert!(matches!(
        Ledger::read_snapshot(b"not a ledger snapshot"),
        Err(LedgerSnapshotError::BadMagic)
    ));
}

#[test]
fn truncated_snapshot_is_rejected() {
    let bytes = build_ledger().write_snapshot();
    assert!(Ledger::read_snapshot(&bytes[..bytes.len() - 1]).is_err());
}
