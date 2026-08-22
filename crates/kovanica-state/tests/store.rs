//! Incremental append-only ledger log: reopen after appends matches a
//! full snapshot, and the file grows instead of being rewritten.

use std::fs;

use kovanica_state::{
    HalvingSchedule, KeyPair, Ledger, LedgerStore, OutPoint, Transaction, TxOutput,
    DEFAULT_HALVING_ERA,
};

const K: u16 = 3;
const SUBSIDY: u64 = 1_000;
const SCHEDULE: HalvingSchedule = HalvingSchedule::new(SUBSIDY, DEFAULT_HALVING_ERA);

fn tmp(name: &str) -> String {
    let path = std::env::temp_dir().join(format!("kovanica-log-{name}-{}", std::process::id()));
    let _ = fs::remove_file(&path);
    path.to_string_lossy().into_owned()
}

fn build() -> Ledger {
    let alice = KeyPair::from_u64(1);
    let bob = KeyPair::from_u64(2);
    let coinbase = Transaction::coinbase(
        vec![TxOutput::new(500, alice.address())],
        b"genesis".to_vec(),
    );
    let coin = OutPoint::new(coinbase.id(), 0);
    let mut ledger = Ledger::new(K, SCHEDULE, &[coinbase]).unwrap();
    let genesis = ledger.genesis();
    let pay = Transaction::signed(
        &[(coin, &alice)],
        vec![
            TxOutput::new(300, bob.address()),
            TxOutput::new(200, alice.address()),
        ],
        Vec::new(),
    );
    ledger.insert(vec![genesis], 1, 1, 0, &[pay]).unwrap();
    ledger
}

#[test]
fn create_then_open_roundtrips_the_ledger() {
    let path = tmp("roundtrip");
    let ledger = build();
    LedgerStore::create(&path, &ledger).unwrap();
    let (_store, restored) = LedgerStore::open(&path).unwrap();
    assert_eq!(restored.dag().linearize(), ledger.dag().linearize());
    assert_eq!(restored.dag().tips(), ledger.dag().tips());
    assert_eq!(
        restored
            .ledger_state()
            .balance(&KeyPair::from_u64(2).address()),
        300
    );
    let _ = fs::remove_file(&path);
}

#[test]
fn append_extends_the_log_without_rewriting() {
    let path = tmp("append");
    let alice = KeyPair::from_u64(1);
    let bob = KeyPair::from_u64(2);
    let carol = KeyPair::from_u64(3);
    let coinbase = Transaction::coinbase(
        vec![TxOutput::new(500, alice.address())],
        b"genesis".to_vec(),
    );
    let coin = OutPoint::new(coinbase.id(), 0);
    let mut ledger = Ledger::new(K, SCHEDULE, &[coinbase]).unwrap();
    let mut store = LedgerStore::create(&path, &ledger).unwrap();
    let size_after_genesis = fs::metadata(&path).unwrap().len();

    let genesis = ledger.genesis();
    let pay = Transaction::signed(
        &[(coin, &alice)],
        vec![
            TxOutput::new(300, bob.address()),
            TxOutput::new(200, alice.address()),
        ],
        Vec::new(),
    );
    let change = OutPoint::new(pay.id(), 1);
    let b1 = ledger.insert(vec![genesis], 1, 1, 0, &[pay]).unwrap();
    store.append(ledger.dag().block(&b1).unwrap()).unwrap();
    let size_after_first = fs::metadata(&path).unwrap().len();
    assert!(
        size_after_first > size_after_genesis,
        "append should grow the file ({size_after_genesis} -> {size_after_first})"
    );

    let pay2 = Transaction::signed(
        &[(change, &alice)],
        vec![TxOutput::new(200, carol.address())],
        Vec::new(),
    );
    let b2 = ledger.insert(vec![b1], 1, 2, 0, &[pay2]).unwrap();
    store.append(ledger.dag().block(&b2).unwrap()).unwrap();
    let size_after_second = fs::metadata(&path).unwrap().len();
    assert!(size_after_second > size_after_first);

    drop(store);
    let (_store, restored) = LedgerStore::open(&path).unwrap();
    assert_eq!(restored.dag().linearize(), ledger.dag().linearize());
    assert_eq!(restored.ledger_state().balance(&carol.address()), 200);
    let _ = fs::remove_file(&path);
}

#[test]
fn truncated_header_is_an_error() {
    let path = tmp("trunc");
    fs::write(&path, b"KV").unwrap();
    let err = match LedgerStore::open(&path) {
        Err(e) => e,
        Ok(_) => panic!("truncated header should not open"),
    };
    assert!(
        matches!(
            err,
            kovanica_state::StoreError::Truncated | kovanica_state::StoreError::Io(_)
        ),
        "got {err}"
    );
    let _ = fs::remove_file(&path);
}
