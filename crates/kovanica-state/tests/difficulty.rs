//! Integration: consensus-enforced difficulty composed with the UTXO ledger.
//!
//! `Ledger::set_difficulty` turns on the DAG's difficulty rules (see
//! `kovanica-dag`'s `tests/difficulty.rs` for the enforcement itself). Here we
//! check the rules compose with the stateful ledger: a block whose `work` equals
//! the target the DAG implies (from `Dag::next_work_target`) and whose timestamp
//! is monotone is admitted, while an off-target block is rejected before it
//! enters the DAG — the difficulty check surfaces as a `LedgerInsertError::Dag`.

use kovanica_dag::{DagError, Retarget};
use kovanica_state::{
    HalvingSchedule, Ledger, LedgerInsertError, Transaction, TxOutput, DEFAULT_HALVING_ERA,
};

const K: u16 = 3;
const SUBSIDY: u64 = 1_000;
const SCHEDULE: HalvingSchedule = HalvingSchedule::new(SUBSIDY, DEFAULT_HALVING_ERA);

fn retarget() -> Retarget {
    Retarget {
        target_interval_ms: 1_000,
        window: 4,
        max_factor: 4,
        min_work: 1,
    }
}

/// A difficulty-enforcing ledger with an empty-payload-friendly genesis.
fn difficulty_ledger() -> Ledger {
    let coinbase = Transaction::coinbase(
        vec![TxOutput::new(
            SUBSIDY,
            kovanica_state::KeyPair::from_u64(1).address(),
        )],
        b"genesis".to_vec(),
    );
    let mut ledger = Ledger::new(K, SCHEDULE, &[coinbase]).expect("valid genesis");
    ledger.set_difficulty(retarget());
    ledger
}

#[test]
fn target_work_blocks_are_accepted_wrong_work_is_rejected() {
    let mut ledger = difficulty_ledger();

    // Build a chain of empty blocks, each mined at exactly the DAG's target work
    // and one target-interval apart. They must all be admitted.
    let mut tip = ledger.genesis();
    for i in 1..=6u64 {
        let parents = ledger.dag().tips();
        let work = ledger
            .dag()
            .next_work_target(&parents)
            .expect("difficulty is enabled");
        tip = ledger
            .insert(parents, work, i * 1_000, 0, &[])
            .unwrap_or_else(|e| panic!("on-target block {i} should be accepted: {e:?}"));
    }
    assert_eq!(ledger.dag().len(), 7); // genesis + 6

    // A block on the current tip carrying the wrong work is rejected, and the
    // ledger/DAG are left unchanged.
    let parents = ledger.dag().tips();
    let target = ledger.dag().next_work_target(&parents).unwrap();
    let before = ledger.dag().len();
    let err = ledger
        .insert(parents, target + 1, 7_000, 0, &[])
        .expect_err("off-target work must be rejected");
    assert!(
        matches!(
            err,
            LedgerInsertError::Dag(DagError::DifficultyMismatch { .. })
        ),
        "expected a difficulty mismatch, got {err:?}"
    );
    assert_eq!(ledger.dag().len(), before, "rejected block was not added");
    let _ = tip;
}

#[test]
fn a_backdated_block_is_rejected_by_the_ledger() {
    let mut ledger = difficulty_ledger();
    let parents = ledger.dag().tips();
    let work = ledger.dag().next_work_target(&parents).unwrap();
    let b1 = ledger.insert(parents, work, 5_000, 0, &[]).unwrap();

    // A child timestamped before its parent is rejected on the difficulty rule.
    let work = ledger.dag().next_work_target(&[b1]).unwrap();
    let err = ledger
        .insert(vec![b1], work, 4_000, 0, &[])
        .expect_err("a backdated block must be rejected");
    assert!(
        matches!(
            err,
            LedgerInsertError::Dag(DagError::NonMonotonicTimestamp { .. })
        ),
        "expected a non-monotonic timestamp error, got {err:?}"
    );
}
