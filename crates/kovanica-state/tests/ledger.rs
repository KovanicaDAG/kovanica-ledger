//! Integration tests for the UTXO ledger over a real GHOSTDAG DAG.
//!
//! These exercise the property that makes this a *DAG* ledger: transactions are
//! applied in `kovanica_dag`'s deterministic linearized order, so
//!
//! * the final state is a pure function of the DAG (independent of the order in
//!   which parallel blocks were inserted), and
//! * a double-spend split across two parallel blocks is resolved deterministically
//!   — exactly one spend takes effect, decided by the linearization.

use kovanica_dag::{Block, BlockId, Dag};
use kovanica_state::{
    apply_dag, encode_block_payload, Address, KeyPair, LedgerError, OutPoint, Transaction,
    TxOutput, UtxoSet,
};

const SUBSIDY: u64 = 1_000;

/// Build a DAG whose genesis coinbase mints `funding` to `owner`. Returns the
/// DAG, the genesis id, and the outpoint of the minted coinbase output.
fn dag_funding(owner: &KeyPair, funding: u64) -> (Dag, BlockId, OutPoint) {
    let coinbase = Transaction::coinbase(
        vec![TxOutput::new(funding, owner.address())],
        b"genesis".to_vec(),
    );
    let coin = OutPoint::new(coinbase.id(), 0);
    let genesis = Block::genesis(1, 0, 0, encode_block_payload(&[coinbase]));
    let genesis_id = genesis.id();
    (Dag::new(3, genesis), genesis_id, coin)
}

/// A block carrying exactly the given transactions on the given parents.
fn tx_block(parents: &[BlockId], txs: &[Transaction]) -> Block {
    Block::new(parents.to_vec(), 1, 0, 0, encode_block_payload(txs))
}

/// A canonical, order-independent snapshot of a UTXO set for equality checks.
fn snapshot(utxo: &UtxoSet) -> Vec<(OutPoint, u64, Address)> {
    let mut rows: Vec<(OutPoint, u64, Address)> =
        utxo.iter().map(|(op, o)| (*op, o.value, o.owner)).collect();
    rows.sort_by_key(|row| row.0);
    rows
}

#[test]
fn genesis_coinbase_funds_an_address() {
    let alice = KeyPair::from_u64(1);
    let (dag, _g, _coin) = dag_funding(&alice, 500);

    let run = apply_dag(&dag, SUBSIDY);
    assert!(run.rejected.is_empty());
    assert_eq!(run.accepted.len(), 1); // just genesis
    assert_eq!(run.utxo.balance(&alice.address()), 500);
    assert_eq!(run.utxo.total_value(), 500);
}

#[test]
fn chained_transfers_apply_in_order() {
    let alice = KeyPair::from_u64(1);
    let bob = KeyPair::from_u64(2);
    let carol = KeyPair::from_u64(3);
    let (mut dag, genesis, coin) = dag_funding(&alice, 500);

    // alice -> bob (100) with 400 change back to alice, then bob -> carol (100),
    // across two chained blocks. Fees are zero so value is fully conserved.
    let a_to_b = Transaction::signed(
        &[(coin, &alice)],
        vec![
            TxOutput::new(100, bob.address()),
            TxOutput::new(400, alice.address()),
        ],
        vec![],
    );
    let bob_coin = OutPoint::new(a_to_b.id(), 0); // output index 0 is Bob's
    let b1 = dag.insert(tx_block(&[genesis], &[a_to_b])).unwrap();

    let b_to_c = Transaction::signed(
        &[(bob_coin, &bob)],
        vec![TxOutput::new(100, carol.address())],
        vec![],
    );
    dag.insert(tx_block(&[b1], &[b_to_c])).unwrap();

    let run = apply_dag(&dag, SUBSIDY);
    assert!(run.rejected.is_empty());
    assert_eq!(run.utxo.balance(&alice.address()), 400); // 500 − 100
    assert_eq!(run.utxo.balance(&bob.address()), 0); // received 100, forwarded 100
    assert_eq!(run.utxo.balance(&carol.address()), 100);
}

#[test]
fn double_spend_across_parallel_blocks_resolved_by_linearization() {
    // Alice is funded once, then TWO parallel blocks each spend that same output:
    // one pays Bob, the other pays Carol. Both are individually valid; GHOSTDAG's
    // linearization picks a deterministic winner and the loser is rejected.
    let alice = KeyPair::from_u64(1);
    let bob = KeyPair::from_u64(2);
    let carol = KeyPair::from_u64(3);
    let (mut dag, genesis, coin) = dag_funding(&alice, 500);

    let pay_bob = Transaction::signed(
        &[(coin, &alice)],
        vec![TxOutput::new(500, bob.address())],
        b"to-bob".to_vec(),
    );
    let pay_carol = Transaction::signed(
        &[(coin, &alice)],
        vec![TxOutput::new(500, carol.address())],
        b"to-carol".to_vec(),
    );

    let block_bob = dag.insert(tx_block(&[genesis], &[pay_bob])).unwrap();
    let block_carol = dag.insert(tx_block(&[genesis], &[pay_carol])).unwrap();
    // A merging block references both conflicting tips (empty payload).
    dag.insert(tx_block(&[block_bob, block_carol], &[]))
        .unwrap();

    let run = apply_dag(&dag, SUBSIDY);

    // Exactly one of the two conflicting blocks is rejected as a double spend.
    let rejected: Vec<BlockId> = run.rejected.iter().map(|(id, _)| *id).collect();
    assert_eq!(
        rejected.len(),
        1,
        "exactly one conflicting block is rejected"
    );
    let loser = rejected[0];
    assert!(loser == block_bob || loser == block_carol);
    assert!(matches!(run.rejected[0].1, LedgerError::MissingInput(op) if op == coin));

    // The winner is whichever of the two comes first in the linearization.
    let order = dag.linearize();
    let winner = if order.iter().position(|b| *b == block_bob)
        < order.iter().position(|b| *b == block_carol)
    {
        block_bob
    } else {
        block_carol
    };
    assert_ne!(winner, loser);
    assert!(run.accepted.contains(&winner));

    // Total value is conserved: the 500 lands with exactly one recipient.
    let bob_bal = run.utxo.balance(&bob.address());
    let carol_bal = run.utxo.balance(&carol.address());
    assert_eq!(bob_bal + carol_bal, 500);
    assert_eq!(run.utxo.total_value(), 500);
    assert_eq!(run.utxo.balance(&alice.address()), 0);
}

#[test]
fn final_state_is_independent_of_insertion_order() {
    // Build the same logical DAG two ways — inserting the two parallel blocks in
    // opposite order — and confirm the ledger's final state is identical. This
    // pins that consensus, not arrival order, decides the ledger.
    let alice = KeyPair::from_u64(1);
    let bob = KeyPair::from_u64(2);
    let carol = KeyPair::from_u64(3);

    let build = |swap: bool| -> UtxoSet {
        let (mut dag, genesis, coin) = dag_funding(&alice, 500);
        let pay_bob = Transaction::signed(
            &[(coin, &alice)],
            vec![TxOutput::new(500, bob.address())],
            b"to-bob".to_vec(),
        );
        let pay_carol = Transaction::signed(
            &[(coin, &alice)],
            vec![TxOutput::new(500, carol.address())],
            b"to-carol".to_vec(),
        );
        let (b_bob, b_carol) = if swap {
            let c = dag.insert(tx_block(&[genesis], &[pay_carol])).unwrap();
            let b = dag.insert(tx_block(&[genesis], &[pay_bob])).unwrap();
            (b, c)
        } else {
            let b = dag.insert(tx_block(&[genesis], &[pay_bob])).unwrap();
            let c = dag.insert(tx_block(&[genesis], &[pay_carol])).unwrap();
            (b, c)
        };
        dag.insert(tx_block(&[b_bob, b_carol], &[])).unwrap();
        apply_dag(&dag, SUBSIDY).utxo
    };

    assert_eq!(snapshot(&build(false)), snapshot(&build(true)));
}

#[test]
fn invalid_block_is_rejected_without_halting_the_ledger() {
    // A block with a forged spend is rejected, but a later valid block still
    // applies — an invalid block has no effect and does not stop the run.
    let alice = KeyPair::from_u64(1);
    let bob = KeyPair::from_u64(2);
    let mallory = KeyPair::from_u64(9);
    let (mut dag, genesis, coin) = dag_funding(&alice, 500);

    // Mallory forges a spend of Alice's coin (bad signature).
    let forged = Transaction::signed(
        &[(coin, &mallory)],
        vec![TxOutput::new(500, mallory.address())],
        b"forge".to_vec(),
    );
    let bad = dag.insert(tx_block(&[genesis], &[forged])).unwrap();

    // Alice legitimately pays Bob, building on the bad block's tip.
    let legit = Transaction::signed(
        &[(coin, &alice)],
        vec![TxOutput::new(300, bob.address())],
        b"legit".to_vec(),
    );
    let good = dag.insert(tx_block(&[bad], &[legit])).unwrap();

    let run = apply_dag(&dag, SUBSIDY);
    assert!(run.accepted.contains(&good));
    assert!(run
        .rejected
        .iter()
        .any(|(id, e)| *id == bad && matches!(e, LedgerError::BadSignature { .. })));
    assert_eq!(run.utxo.balance(&bob.address()), 300);
    assert_eq!(run.utxo.balance(&mallory.address()), 0);
}
