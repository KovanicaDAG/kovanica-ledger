//! # kovanica-state
//!
//! The **UTXO ledger** of the Kovanica Ledger: the state layer that turns the
//! GHOSTDAG-ordered block DAG (see `kovanica_dag`) into an actual, spendable
//! ledger. Blocks carry transactions; consensus decides their order; this crate
//! applies them.
//!
//! It provides:
//!
//! * [`Transaction`] / [`TxId`] / [`OutPoint`] / [`TxInput`] / [`TxOutput`] —
//!   the UTXO transaction model, with a canonical, length-prefixed encoding that
//!   doubles as a block's opaque payload
//!   ([`encode_block_payload`] / [`decode_block_payload`]).
//! * [`KeyPair`] / [`Address`] / [`verify`] — ed25519 spend authorisation.
//! * [`UtxoSet`] — the ledger state (unspent outputs).
//! * [`apply_block`] — the strict, atomic state transition for one block.
//! * [`apply_dag`] — the bridge to consensus: linearize a
//!   [`kovanica_dag::Dag`] and apply every block's transactions in that order,
//!   so conflicting spends across parallel blocks are resolved deterministically
//!   by GHOSTDAG.
//! * [`validate_block_payload`] / [`TxStructureValidator`] — context-free
//!   structural validation of a block's transactions, installable on a
//!   [`kovanica_dag::Dag`] so malformed blocks are rejected *at insert time*
//!   (see [`validation`]). Stateful rules stay in [`apply_block`].
//! * [`Ledger`] — a DAG that maintains the **per-block UTXO state** each block
//!   induces, built incrementally from each block's selected parent. It performs
//!   *stateful* validation at insert (a block invalid in its own view never
//!   enters the DAG) and its full state matches [`apply_dag`]. It also
//!   **persists**: [`Ledger::write_snapshot`] / [`Ledger::read_snapshot`]
//!   round-trip the whole ledger by replaying blocks (state is recomputed, not
//!   trusted from disk), built on [`kovanica_dag::Dag::write_snapshot`].
//!
//! ## Quick tour
//!
//! ```
//! use kovanica_dag::{Block, Dag};
//! use kovanica_state::{
//!     apply_dag, encode_block_payload, KeyPair, OutPoint, Transaction, TxOutput,
//! };
//!
//! // Actors.
//! let miner = KeyPair::from_u64(1);
//! let alice = KeyPair::from_u64(2);
//! let bob = KeyPair::from_u64(3);
//!
//! // Genesis carries a coinbase that mints 100 to the miner (subsidy = 100).
//! let genesis_cb = Transaction::coinbase(vec![TxOutput::new(100, miner.address())], b"genesis".to_vec());
//! let genesis_cb_id = genesis_cb.id();
//! let genesis = Block::genesis(1, 0, 0, encode_block_payload(&[genesis_cb]));
//! let genesis_id = genesis.id();
//! let mut dag = Dag::new(3, genesis);
//!
//! // Block 1: the miner sends 70 to Alice (10 fee), spending the coinbase output.
//! let coin = OutPoint::new(genesis_cb_id, 0);
//! let to_alice = Transaction::signed(&[(coin, &miner)], vec![TxOutput::new(70, alice.address())], vec![]);
//! let to_alice_id = to_alice.id();
//! let b1 = Block::new(vec![genesis_id], 1, 1, 0, encode_block_payload(&[to_alice]));
//! let b1_id = dag.insert(b1).unwrap();
//!
//! // Block 2: Alice forwards 70 to Bob.
//! let alice_coin = OutPoint::new(to_alice_id, 0);
//! let to_bob = Transaction::signed(&[(alice_coin, &alice)], vec![TxOutput::new(70, bob.address())], vec![]);
//! dag.insert(Block::new(vec![b1_id], 1, 2, 0, encode_block_payload(&[to_bob]))).unwrap();
//!
//! // Apply the whole DAG in GHOSTDAG order (subsidy = 100 per block).
//! let run = apply_dag(&dag, 100);
//! assert_eq!(run.rejected.len(), 0);
//! assert_eq!(run.utxo.balance(&bob.address()), 70);
//! assert_eq!(run.utxo.balance(&alice.address()), 0);
//! ```

pub mod keys;
pub mod ledger;
pub mod store;
pub mod tx;
pub mod utxo;
pub mod validation;

pub use keys::{verify, Address, KeyPair};
pub use ledger::{
    apply_block, apply_dag, BlockSummary, Ledger, LedgerError, LedgerInsertError, LedgerRun,
    LedgerSnapshotError, HalvingSchedule, DEFAULT_HALVING_ERA,
};
pub use store::{LedgerStore, StoreError};
pub use tx::{
    decode_block_payload, encode_block_payload, DecodeError, OutPoint, Sig, Transaction, TxId,
    TxInput, TxOutput,
};
pub use utxo::UtxoSet;
pub use validation::{validate_block_payload, BlockValidationError, TxStructureValidator, MAX_TX_SIZE, MAX_BLOCK_PAYLOAD_SIZE, MAX_TXS_PER_BLOCK};
