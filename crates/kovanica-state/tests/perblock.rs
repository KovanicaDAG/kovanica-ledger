//! Integration tests for [`Ledger`]: per-block UTXO state and stateful
//! validation at insert.
//!
//! The `Ledger` stores, for every block, the UTXO state of that block's own
//! view, built incrementally from its selected parent. Two properties matter:
//!
//! * **consistency** — the full ledger state (and each block's view state)
//!   matches the batch `apply_dag`; and
//! * **stateful validation at insert** — a block invalid in its own view is
//!   rejected and never enters the DAG, while two *parallel* conflicting blocks
//!   are both admitted (their conflict is resolved only where they are merged).

use kovanica_dag::DagError;
use kovanica_state::{
    apply_dag, Address, HalvingSchedule, KeyPair, Ledger, LedgerError, LedgerInsertError, OutPoint,
    Transaction, TxOutput, UtxoSet, DEFAULT_HALVING_ERA,
};

const K: u16 = 3;
const SUBSIDY: u64 = 1_000;
const SCHEDULE: HalvingSchedule = HalvingSchedule::new(SUBSIDY, DEFAULT_HALVING_ERA);

/// A ledger whose genesis coinbase mints `funding` to `owner`. Returns the
/// ledger and the outpoint of the minted coinbase output.
fn funded_ledger(owner: &KeyPair, funding: u64) -> (Ledger, OutPoint) {
    let coinbase = Transaction::coinbase(
        vec![TxOutput::new(funding, owner.address())],
        b"genesis".to_vec(),
    );
    let coin = OutPoint::new(coinbase.id(), 0);
    let ledger = Ledger::new(K, SCHEDULE, &[coinbase]).expect("valid genesis");
    (ledger, coin)
}

/// A signed transfer of `value` from `from` (spending `coin`) to `to`, with the
/// remainder returned to `from` as change so no value is burned as fees.
fn transfer(coin: OutPoint, from: &KeyPair, to: &Address, value: u64, funding: u64) -> Transaction {
    let mut outputs = vec![TxOutput::new(value, *to)];
    if funding > value {
        outputs.push(TxOutput::new(funding - value, from.address()));
    }
    Transaction::signed(&[(coin, from)], outputs, Vec::new())
}

/// Order-independent snapshot of a UTXO set for equality checks.
fn snapshot(utxo: &UtxoSet) -> Vec<(OutPoint, u64, Address)> {
    let mut rows: Vec<(OutPoint, u64, Address)> =
        utxo.iter().map(|(op, o)| (*op, o.value, o.owner)).collect();
    rows.sort_by_key(|row| row.0);
    rows
}

#[test]
fn genesis_state_reflects_the_coinbase() {
    let alice = KeyPair::from_u64(1);
    let (ledger, _coin) = funded_ledger(&alice, 500);
    let genesis_state = ledger.state(&ledger.genesis()).unwrap();
    assert_eq!(genesis_state.balance(&alice.address()), 500);
    assert_eq!(snapshot(&ledger.ledger_state()), snapshot(genesis_state));
}

#[test]
fn per_block_state_advances_with_each_block() {
    let alice = KeyPair::from_u64(1);
    let bob = KeyPair::from_u64(2);
    let (mut ledger, coin) = funded_ledger(&alice, 500);
    let genesis = ledger.genesis();

    let tx = transfer(coin, &alice, &bob.address(), 200, 500);
    let b1 = ledger
        .insert(vec![genesis], 1, 0, 0, &[tx])
        .expect("valid transfer");

    // Genesis's view is unchanged; b1's view reflects the transfer.
    assert_eq!(ledger.state(&genesis).unwrap().balance(&bob.address()), 0);
    let b1_state = ledger.state(&b1).unwrap();
    assert_eq!(b1_state.balance(&bob.address()), 200);
    assert_eq!(b1_state.balance(&alice.address()), 300);
}

#[test]
fn double_spending_an_ancestor_output_is_rejected_at_insert() {
    // b1 spends the genesis coin (alice → bob). b2, built on b1, tries to spend
    // the SAME genesis coin again — in b2's view that output is already spent by
    // its ancestor b1, so b2 is rejected at insert and never enters the DAG.
    let alice = KeyPair::from_u64(1);
    let bob = KeyPair::from_u64(2);
    let (mut ledger, coin) = funded_ledger(&alice, 500);
    let genesis = ledger.genesis();

    let b1 = ledger
        .insert(
            vec![genesis],
            1,
            0,
            0,
            &[transfer(coin, &alice, &bob.address(), 500, 500)],
        )
        .expect("first spend is valid");

    let blocks_before = ledger.dag().len();
    let err = ledger
        .insert(
            vec![b1],
            1,
            0,
            0,
            &[transfer(coin, &alice, &bob.address(), 500, 500)],
        )
        .unwrap_err();
    assert_eq!(
        err,
        LedgerInsertError::State(LedgerError::MissingInput(coin))
    );
    assert_eq!(
        ledger.dag().len(),
        blocks_before,
        "rejected block not added"
    );
}

#[test]
fn forged_signature_is_rejected_at_insert() {
    let alice = KeyPair::from_u64(1);
    let mallory = KeyPair::from_u64(9);
    let (mut ledger, coin) = funded_ledger(&alice, 500);
    let genesis = ledger.genesis();

    // Mallory signs a spend of Alice's coin.
    let forged = Transaction::signed(
        &[(coin, &mallory)],
        vec![TxOutput::new(500, mallory.address())],
        b"forge".to_vec(),
    );
    let err = ledger
        .insert(vec![genesis], 1, 0, 0, &[forged])
        .unwrap_err();
    assert!(matches!(
        err,
        LedgerInsertError::State(LedgerError::BadSignature { .. })
    ));
    assert_eq!(ledger.dag().len(), 1, "only genesis remains");
}

#[test]
fn parallel_conflicting_blocks_are_both_admitted_then_resolved_at_merge() {
    // Two parallel blocks each spend the genesis coin — one pays Bob, one pays
    // Carol. Each is valid in its own view (neither sees the other), so both are
    // admitted. A block merging them applies only the first in mergeset order;
    // the other conflicts and does not take effect.
    let alice = KeyPair::from_u64(1);
    let bob = KeyPair::from_u64(2);
    let carol = KeyPair::from_u64(3);
    let (mut ledger, coin) = funded_ledger(&alice, 500);
    let genesis = ledger.genesis();

    let to_bob = ledger
        .insert(
            vec![genesis],
            1,
            0,
            0,
            &[transfer(coin, &alice, &bob.address(), 500, 500)],
        )
        .expect("valid in its own view");
    let to_carol = ledger
        .insert(
            vec![genesis],
            1,
            0,
            0,
            &[transfer(coin, &alice, &carol.address(), 500, 500)],
        )
        .expect("also valid in its own view");

    // Both parallel blocks entered the DAG.
    assert!(ledger.dag().contains(&to_bob));
    assert!(ledger.dag().contains(&to_carol));

    // Merge them (empty payload). In the merge's view exactly one spend wins.
    let merge = ledger
        .insert(vec![to_bob, to_carol], 1, 0, 0, &[])
        .expect("merge is valid");
    let merge_state = ledger.state(&merge).unwrap();
    let bob_bal = merge_state.balance(&bob.address());
    let carol_bal = merge_state.balance(&carol.address());
    assert_eq!(bob_bal + carol_bal, 500, "exactly one recipient is paid");
    assert!(bob_bal == 0 || carol_bal == 0);
}

#[test]
fn ledger_state_matches_batch_apply_dag() {
    // Build a non-trivial DAG through the Ledger, then confirm the incrementally
    // maintained full state equals the batch apply_dag over the same DAG — the
    // core consistency guarantee of per-block state.
    let alice = KeyPair::from_u64(1);
    let bob = KeyPair::from_u64(2);
    let carol = KeyPair::from_u64(3);
    let (mut ledger, coin) = funded_ledger(&alice, 500);
    let genesis = ledger.genesis();

    // A chain: alice → bob (with change), then bob forwards to carol.
    let a_to_b = transfer(coin, &alice, &bob.address(), 300, 500);
    let bob_coin = OutPoint::new(a_to_b.id(), 0);
    let b1 = ledger.insert(vec![genesis], 1, 0, 0, &[a_to_b]).unwrap();
    let b_to_c = Transaction::signed(
        &[(bob_coin, &bob)],
        vec![TxOutput::new(300, carol.address())],
        Vec::new(),
    );
    let b2 = ledger.insert(vec![b1], 1, 0, 0, &[b_to_c]).unwrap();

    // A parallel side block off b1: alice spends her change output (index 1 of
    // a_to_b) to carol. Valid in b1's view, where the change exists.
    let alice_change = OutPoint::new(bob_coin.tx, 1);
    let side = Transaction::signed(
        &[(alice_change, &alice)],
        vec![TxOutput::new(200, carol.address())],
        b"side".to_vec(),
    );
    let side_block = ledger.insert(vec![b1], 1, 0, 0, &[side]).unwrap();

    // Merge the two tips.
    ledger.insert(vec![b2, side_block], 1, 0, 0, &[]).unwrap();

    let incremental = ledger.ledger_state();
    let batch = apply_dag(ledger.dag(), SUBSIDY).utxo;
    assert_eq!(snapshot(&incremental), snapshot(&batch));
}

#[test]
fn insert_reports_dag_errors() {
    let alice = KeyPair::from_u64(1);
    let (mut ledger, _coin) = funded_ledger(&alice, 500);

    // A parent that isn't in the DAG surfaces as a DAG error, not a state error.
    let bogus_parent = kovanica_dag::Block::genesis(1, 0, 0, b"nope".to_vec()).id();
    let err = ledger.insert(vec![bogus_parent], 1, 0, 0, &[]).unwrap_err();
    assert!(matches!(
        err,
        LedgerInsertError::Dag(DagError::MissingParent(_))
    ));
}
