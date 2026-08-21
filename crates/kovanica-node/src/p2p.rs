//! Continuous in-process gossip: a mesh of named [`Node`]s, a directed peer
//! graph, and a delayed relay queue.
//!
//! [`crate::net::gossip`] is a one-shot pull-all between two nodes. This module
//! is the next slice: each node has a **peer set**; a produced block or a
//! submitted transaction is announced to those peers; receivers **relay**
//! onward, bounded by a per-node seen-set so the flood terminates; `hello`
//! messages advertise a node's current peers so missing edges close (a
//! minimal **peer discovery**). Time is discrete ([`Mesh::tick`]) so tests
//! stay deterministic — there is no thread, socket, or wall-clock wait here.
//!
//! TCP one-shot sync (the on-wire path) lives in [`crate::net`]: a seed
//! accepts on `KOVANICA_LISTEN` (default `:9000`) and writes every block;
//! a clone pulls with `KOVANICA_PEERS=explorer.kovanica.online:9000`.
//! Long-lived relay sessions are [`crate::relay`] — tests only, not the
//! explorer loop.

use std::collections::{BTreeMap, BTreeSet, HashSet};

use kovanica_dag::{Block, BlockId};
use kovanica_state::{encode_block_payload, Address, Transaction, TxId};

use crate::node::{BlockRecord, Node, NodeError};

/// Why a mesh operation failed.
#[derive(Debug)]
pub enum P2pError {
    /// No node is registered under that name.
    UnknownNode(String),
    /// The underlying node rejected the operation.
    Node(NodeError),
}

impl core::fmt::Display for P2pError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            P2pError::UnknownNode(n) => write!(f, "unknown node {n}"),
            P2pError::Node(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for P2pError {}

impl From<NodeError> for P2pError {
    fn from(e: NodeError) -> Self {
        P2pError::Node(e)
    }
}

/// A delivered gossip event, for tests and logging.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GossipEvent {
    /// Discrete mesh time at delivery.
    pub at: u64,
    /// Sending node.
    pub from: String,
    /// Receiving node.
    pub to: String,
    /// What was relayed.
    pub kind: GossipKind,
}

/// The payload kind of a [`GossipEvent`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GossipKind {
    /// Peer-discovery hello (advertises the sender's current peer set).
    Hello,
    /// A block record.
    Block,
    /// A mempool transaction.
    Tx,
}

enum Envelope {
    Hello { advertised: Vec<String> },
    Block { record: BlockRecord },
    Tx { tx: Transaction },
}

struct Queued {
    due: u64,
    from: String,
    to: String,
    envelope: Envelope,
}

/// Reconstruct the content-addressed id a [`BlockRecord`] will have on insert.
/// Used as the flood seen-set key so a node relays a block at most once.
fn record_id(record: &BlockRecord) -> BlockId {
    Block::new(
        record.parents.clone(),
        record.work,
        record.timestamp_ms,
        record.nonce,
        encode_block_payload(&record.txs),
    )
    .id()
}

/// An in-process overlay of named nodes with continuous gossip.
///
/// Nodes are stored by a stable string name (BTreeMap, so iteration order is
/// deterministic). The peer graph is directed: `connect("a","b")` means `a`
/// announces to `b`. Hellos add the reverse edge and any advertised neighbours.
#[derive(Default)]
pub struct Mesh {
    nodes: BTreeMap<String, Node>,
    peers: BTreeMap<String, BTreeSet<String>>,
    queue: Vec<Queued>,
    seen_blocks: BTreeMap<String, HashSet<BlockId>>,
    seen_txs: BTreeMap<String, HashSet<TxId>>,
    /// Discrete time. Advanced by [`Mesh::tick`].
    now: u64,
    events: Vec<GossipEvent>,
}

impl Mesh {
    /// An empty mesh.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register `node` under `name`. Replaces any previous node of that name.
    pub fn add(&mut self, name: impl Into<String>, node: Node) {
        let name = name.into();
        self.nodes.insert(name.clone(), node);
        self.peers.entry(name.clone()).or_default();
        self.seen_blocks.entry(name.clone()).or_default();
        self.seen_txs.entry(name).or_default();
    }

    /// Registered node names, sorted.
    pub fn names(&self) -> Vec<String> {
        self.nodes.keys().cloned().collect()
    }

    /// Discrete mesh time.
    pub fn now(&self) -> u64 {
        self.now
    }

    /// Envelopes waiting to be delivered.
    pub fn queued(&self) -> usize {
        self.queue.len()
    }

    /// Borrow a node by name.
    pub fn node(&self, name: &str) -> Option<&Node> {
        self.nodes.get(name)
    }

    /// Mutably borrow a node by name.
    pub fn node_mut(&mut self, name: &str) -> Option<&mut Node> {
        self.nodes.get_mut(name)
    }

    /// Directed overlay edge: `from` will announce blocks/txs/hellos to `to`.
    /// Enqueues a hello so `to` learns about `from` (and `from`'s peers).
    pub fn connect(&mut self, from: &str, to: &str) -> Result<(), P2pError> {
        self.require(from)?;
        self.require(to)?;
        if from == to {
            return Ok(());
        }
        let inserted = self
            .peers
            .entry(from.to_string())
            .or_default()
            .insert(to.to_string());
        if inserted {
            self.enqueue_hello(from, to);
        }
        Ok(())
    }

    /// Current peers of `name`, in sorted order.
    pub fn peers_of(&self, name: &str) -> Vec<String> {
        self.peers
            .get(name)
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Deliver every message whose `due` time is `<= now`, then advance `now`
    /// by one. Returns how many envelopes were delivered this tick.
    pub fn tick(&mut self) -> usize {
        self.now = self.now.saturating_add(1);
        let mut due = Vec::new();
        let mut rest = Vec::new();
        for q in self.queue.drain(..) {
            if q.due <= self.now {
                due.push(q);
            } else {
                rest.push(q);
            }
        }
        self.queue = rest;
        let n = due.len();
        for q in due {
            self.deliver(q);
        }
        n
    }

    /// [`tick`] until the relay queue is empty (or `limit` ticks). Returns the
    /// number of envelopes delivered. The limit is a safety valve against a
    /// buggy flood; honest meshes drain in O(diameter) ticks.
    pub fn drain(&mut self, limit: u32) -> usize {
        let mut total = 0;
        for _ in 0..limit {
            if self.queue.is_empty() {
                break;
            }
            total += self.tick();
        }
        total
    }

    /// Whether the relay queue is empty (no pending envelopes).
    pub fn is_idle(&self) -> bool {
        self.queue.is_empty()
    }

    /// Produce a block on `name` and announce it to that node's peers.
    pub fn produce(&mut self, name: &str) -> Result<Option<BlockId>, P2pError> {
        self.require(name)?;
        let id = self.nodes.get_mut(name).expect("checked").produce_block()?;
        if let Some(id) = id {
            if let Some(rec) = self.nodes.get(name).and_then(|n| n.block_record(&id)) {
                self.announce_block(name, rec);
            }
        }
        Ok(id)
    }

    /// Produce an empty block on `name` and announce it.
    pub fn produce_empty(&mut self, name: &str) -> Result<BlockId, P2pError> {
        self.require(name)?;
        let id = self
            .nodes
            .get_mut(name)
            .expect("checked")
            .produce_empty()?;
        if let Some(rec) = self.nodes.get(name).and_then(|n| n.block_record(&id)) {
            self.announce_block(name, rec);
        }
        Ok(id)
    }

    /// Immediate send on `name`, then announce the new block.
    pub fn send(
        &mut self,
        name: &str,
        from_seed: u64,
        amount: u64,
        to_seed: u64,
    ) -> Result<BlockId, P2pError> {
        self.require(name)?;
        let sent = self
            .nodes
            .get_mut(name)
            .expect("checked")
            .send(from_seed, amount, to_seed)?;
        if let Some(rec) = self
            .nodes
            .get(name)
            .and_then(|n| n.block_record(&sent.block))
        {
            self.announce_block(name, rec);
        }
        Ok(sent.block)
    }

    /// Queue a transfer in `name`'s mempool and announce the tx to peers.
    pub fn pool(
        &mut self,
        name: &str,
        from_seed: u64,
        amount: u64,
        to_seed: u64,
    ) -> Result<TxId, P2pError> {
        self.require(name)?;
        let id = self
            .nodes
            .get_mut(name)
            .expect("checked")
            .pool(from_seed, amount, to_seed)?;
        if let Some(tx) = self.nodes.get(name).and_then(|n| n.mempool_tx(&id)) {
            self.announce_tx(name, tx);
        }
        Ok(id)
    }

    /// Wallet-signed spend into `name`'s mempool, then announce the tx.
    pub fn submit_signed(
        &mut self,
        name: &str,
        from: Address,
        amount: u64,
        to: Address,
        signature: [u8; 64],
    ) -> Result<TxId, P2pError> {
        self.require(name)?;
        let id = self
            .nodes
            .get_mut(name)
            .expect("checked")
            .submit_signed(from, amount, to, signature)?;
        if let Some(tx) = self.nodes.get(name).and_then(|n| n.mempool_tx(&id)) {
            self.announce_tx(name, tx);
        }
        Ok(id)
    }

    /// Seed actor pays an arbitrary address (explorer faucet) and announces.
    pub fn send_to(
        &mut self,
        name: &str,
        from_seed: u64,
        amount: u64,
        to: Address,
    ) -> Result<BlockId, P2pError> {
        self.require(name)?;
        let sent = self
            .nodes
            .get_mut(name)
            .expect("checked")
            .send_to(from_seed, amount, to)?;
        if let Some(rec) = self
            .nodes
            .get(name)
            .and_then(|n| n.block_record(&sent.block))
        {
            self.announce_block(name, rec);
        }
        Ok(sent.block)
    }

    /// Delivered events, oldest first.
    pub fn events(&self) -> &[GossipEvent] {
        &self.events
    }

    fn require(&self, name: &str) -> Result<(), P2pError> {
        if self.nodes.contains_key(name) {
            Ok(())
        } else {
            Err(P2pError::UnknownNode(name.to_string()))
        }
    }

    fn enqueue_hello(&mut self, from: &str, to: &str) {
        let advertised = self.peers_of(from);
        self.enqueue(from, to, Envelope::Hello { advertised });
    }

    fn announce_block(&mut self, from: &str, record: BlockRecord) {
        let id = record_id(&record);
        self.seen_blocks
            .entry(from.to_string())
            .or_default()
            .insert(id);
        let peers = self.peers_of(from);
        for to in peers {
            self.enqueue(
                from,
                &to,
                Envelope::Block {
                    record: record.clone(),
                },
            );
        }
    }

    fn announce_tx(&mut self, from: &str, tx: Transaction) {
        let id = tx.id();
        self.seen_txs
            .entry(from.to_string())
            .or_default()
            .insert(id);
        let peers = self.peers_of(from);
        for to in peers {
            self.enqueue(from, &to, Envelope::Tx { tx: tx.clone() });
        }
    }

    fn enqueue(&mut self, from: &str, to: &str, envelope: Envelope) {
        if from == to {
            return;
        }
        self.queue.push(Queued {
            due: self.now.saturating_add(1),
            from: from.to_string(),
            to: to.to_string(),
            envelope,
        });
    }

    fn deliver(&mut self, q: Queued) {
        let kind = match &q.envelope {
            Envelope::Hello { .. } => GossipKind::Hello,
            Envelope::Block { .. } => GossipKind::Block,
            Envelope::Tx { .. } => GossipKind::Tx,
        };
        self.events.push(GossipEvent {
            at: self.now,
            from: q.from.clone(),
            to: q.to.clone(),
            kind,
        });
        match q.envelope {
            Envelope::Hello { advertised } => self.on_hello(&q.to, &q.from, advertised),
            Envelope::Block { record } => self.on_block(&q.to, &q.from, record),
            Envelope::Tx { tx } => self.on_tx(&q.to, &q.from, tx),
        }
    }

    fn on_hello(&mut self, to: &str, from: &str, advertised: Vec<String>) {
        if !self.nodes.contains_key(to) {
            return;
        }
        let peers = self.peers.entry(to.to_string()).or_default();
        let new_reverse = peers.insert(from.to_string());
        let mut fresh = Vec::new();
        for p in advertised {
            if p != to && p != from && self.nodes.contains_key(&p) && peers.insert(p.clone()) {
                fresh.push(p);
            }
        }
        if new_reverse {
            self.enqueue_hello(to, from);
        }
        for p in fresh {
            self.enqueue_hello(to, &p);
        }
    }

    fn on_block(&mut self, to: &str, from: &str, record: BlockRecord) {
        let id = record_id(&record);
        let seen = self.seen_blocks.entry(to.to_string()).or_default();
        if !seen.insert(id) {
            return;
        }
        if let Some(node) = self.nodes.get_mut(to) {
            if node.receive_block(record.clone()).is_err() {
                return;
            }
        }
        let peers = self.peers_of(to);
        for nxt in peers {
            if nxt == from {
                continue;
            }
            self.enqueue(
                to,
                &nxt,
                Envelope::Block {
                    record: record.clone(),
                },
            );
        }
    }

    fn on_tx(&mut self, to: &str, from: &str, tx: Transaction) {
        let id = tx.id();
        let seen = self.seen_txs.entry(to.to_string()).or_default();
        if !seen.insert(id) {
            return;
        }
        if let Some(node) = self.nodes.get_mut(to) {
            if node.submit_tx(tx.clone()).is_err() {
                return;
            }
        }
        let peers = self.peers_of(to);
        for nxt in peers {
            if nxt == from {
                continue;
            }
            self.enqueue(to, &nxt, Envelope::Tx { tx: tx.clone() });
        }
    }
}
