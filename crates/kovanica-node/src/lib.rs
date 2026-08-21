//! # kovanica-node
//!
//! A runnable node for the Kovanica DAG ledger. It ties the stack together —
//! `kovanica_dag` (the block DAG + GHOSTDAG consensus) and `kovanica_state` (the
//! UTXO [`Ledger`](kovanica_state::Ledger), signatures, per-block state,
//! snapshots) — behind a small, testable interface.
//!
//! * [`Node`] holds the ledger in memory and exposes the high-level operations:
//!   bring up a genesis, submit signed transfers, query balances / tips, and
//!   save or load a snapshot.
//! * [`rpc::execute_line`] is a line-based command protocol (string in, string
//!   out) — the node's "RPC". The binary wires it to stdin/stdout (`serve`) or
//!   replays a scripted `demo`.
//!
//! Nodes are multi-node aware: a [`Mempool`](mempool::Mempool) holds pending
//! transactions, [`Node::produce_block`] packs the valid ones into a block, and
//! blocks gossip between nodes ([`net::gossip`] in-process, or
//! [`net::serve_blocks`] / [`net::pull_blocks`] over TCP :9000) so peers
//! converge on the same DAG. [`p2p::Mesh`] is the in-process overlay used by
//! tests and the explorer's internal tick. The **only** on-wire path is
//! plaintext TCP (`KOVANICA_LISTEN`, default `0.0.0.0:9000`).
//! Actors are integer *seeds* the node signs for — a demo convenience,
//! not how a real node handles keys (see [`node`]).
//!
//! ```
//! use kovanica_node::{rpc, Node};
//!
//! let mut node = Node::new();
//! // Genesis mints 500 to actor 1; actor 1 sends 200 to actor 2.
//! assert!(rpc::execute_line(&mut node, "genesis 3 1000 500 1").starts_with("ok genesis"));
//! assert!(rpc::execute_line(&mut node, "send 1 200 2").starts_with("ok block"));
//! assert_eq!(rpc::execute_line(&mut node, "balance 1"), "ok 299");
//! assert_eq!(rpc::execute_line(&mut node, "balance 2"), "ok 200");
//! ```

pub mod explorer;
pub mod mempool;
pub mod net;
pub mod node;
pub mod p2p;
pub mod relay;
pub mod rpc;

pub use explorer::serve as serve_explorer;
pub use mempool::Mempool;
pub use net::NetError;
pub use node::{BlockRecord, Node, NodeError, Prepared, Sent};
pub use p2p::{GossipEvent, GossipKind, Mesh, P2pError};
pub use relay::{apply_relay, RelayMsg, RelaySession};
