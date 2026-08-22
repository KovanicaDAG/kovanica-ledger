//! Block dissemination between nodes.
//!
//! Nodes converge on one DAG by exchanging blocks: [`Node::export`] gives a
//! peer's blocks as [`BlockRecord`]s in topological order, and
//! [`Node::receive_block`] re-inserts them (idempotently). Because a block is
//! content-addressed and insertion is deterministic, once two nodes have
//! exchanged blocks both hold the identical DAG, linearization, and UTXO state —
//! and any cross-node conflict (two nodes independently spending the same output)
//! resolves the same way on both.
//!
//! [`gossip`] does one directional catch-up in-process. [`serve_blocks`] /
//! [`pull_blocks`] do the same over a TCP stream — a minimal one-shot "give me
//! all your blocks" sync. [`pull_blocks_timeout`] / [`serve_exchange`] upgrade
//! that to a **framed, bidirectional exchange**: each side reads the peer's
//! framed dump (record count + records — no EOF needed), applies it, then
//! sends its own pre-apply snapshot back, so both ends of one connection walk
//! away with the union. Old servers that close after serving still work: the
//! reply write simply fails and is ignored. Continuous gossip with a peer set
//! and a relay loop lives in [`crate::p2p`]. Records are assumed to arrive in
//! topological order, which [`Node::export`] guarantees.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::time::Duration;

use kovanica_dag::BlockId;
use kovanica_state::{decode_block_payload, encode_block_payload};

use crate::node::{BlockHeader, BlockRecord, Node};

/// Copy every block `from` has into `to`, in topological order (in-process).
/// Returns the number of records applied. Idempotent — already-present blocks
/// are skipped.
pub fn gossip(from: &Node, to: &mut Node) -> Result<usize, NetError> {
    let mut applied = 0;
    for record in from.export() {
        to.receive_block(record)
            .map_err(|e| NetError::Apply(e.to_string()))?;
        applied += 1;
    }
    Ok(applied)
}

/// Serve this node's blocks to one peer over `listener`: accept a single
/// connection, write all block records, and close. The peer reads them with
/// [`pull_blocks`].
pub fn serve_blocks(listener: &TcpListener, node: &Node) -> Result<(), NetError> {
    serve_records(listener, &node.export())
}

/// Serve a pre-computed set of block records to one peer over `listener`. Handy
/// when the serving side must run on another thread: `records` (from
/// [`Node::export`]) is `Send`, whereas a whole node is not.
pub fn serve_records(listener: &TcpListener, records: &[BlockRecord]) -> Result<(), NetError> {
    let (mut stream, _) = listener.accept().map_err(io)?;
    let bytes = encode_records(records);
    stream.write_all(&bytes).map_err(io)?;
    stream.flush().map_err(io)?;
    Ok(())
}

/// Connect to a peer serving via [`serve_blocks`], read its blocks, and apply
/// them to `node`. Returns the number of records applied.
pub fn pull_blocks<A: ToSocketAddrs>(addr: A, node: &mut Node) -> Result<usize, NetError> {
    let mut stream = TcpStream::connect(addr).map_err(io)?;
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).map_err(io)?;
    let records = decode_records(&buf)?;
    let mut applied = 0;
    for record in records {
        node.receive_block(record)
            .map_err(|e| NetError::Apply(e.to_string()))?;
        applied += 1;
    }
    Ok(applied)
}

/// Like [`pull_blocks`] but bounded so a dead peer cannot stall the explorer.
/// Tries every resolved address (IPv4 first).
///
/// Reads a **framed** dump (record count + records — no EOF needed), applies it,
/// then writes our pre-apply snapshot so the peer can learn extra blocks.
/// Write errors are ignored (old peers close after serving).
pub fn pull_blocks_timeout(
    addr: &str,
    node: &mut Node,
    timeout: Duration,
) -> Result<usize, NetError> {
    let mut socks: Vec<_> = addr.to_socket_addrs().map_err(io)?.collect();
    if socks.is_empty() {
        return Err(NetError::Io("no address".into()));
    }
    socks.sort_by_key(|s| if s.is_ipv4() { 0u8 } else { 1 });
    let mut last = NetError::Io("no address".into());
    for sock in socks {
        match TcpStream::connect_timeout(&sock, timeout) {
            Ok(mut stream) => {
                stream.set_read_timeout(Some(timeout)).map_err(io)?;
                stream.set_write_timeout(Some(timeout)).map_err(io)?;
                let mine = encode_records(&node.export());
                let recs = read_records_from(&mut stream)?;
                let applied = apply_decoded(recs, node)?;
                let _ = stream.write_all(&mine);
                let _ = stream.flush();
                return Ok(applied);
            }
            Err(e) => last = io(e),
        }
    }
    Err(last)
}

/// Peer side of the [`pull_blocks_timeout`] exchange: write our dump, then
/// read a framed dump from the other end and apply it. Timeout / EOF / an old
/// client that never writes → 0 records, not an error.
pub fn serve_exchange(
    stream: &mut TcpStream,
    node: &mut Node,
    timeout: Duration,
) -> Result<usize, NetError> {
    stream.set_nonblocking(false).map_err(io)?;
    stream.set_read_timeout(Some(timeout)).map_err(io)?;
    stream.set_write_timeout(Some(timeout)).map_err(io)?;
    let bytes = encode_records(&node.export());
    stream.write_all(&bytes).map_err(io)?;
    stream.flush().map_err(io)?;
    match read_records_from(stream) {
        Ok(recs) => apply_decoded(recs, node),
        Err(NetError::Io(_)) => Ok(0),
        Err(e) => Err(e),
    }
}

fn apply_decoded(records: Vec<BlockRecord>, node: &mut Node) -> Result<usize, NetError> {
    let mut applied = 0;
    for record in records {
        node.receive_block(record)
            .map_err(|e| NetError::Apply(e.to_string()))?;
        applied += 1;
    }
    Ok(applied)
}

fn read_u64<R: Read>(r: &mut R) -> Result<u64, NetError> {
    let mut b = [0u8; 8];
    r.read_exact(&mut b).map_err(io)?;
    Ok(u64::from_le_bytes(b))
}

fn read_record_from<R: Read>(r: &mut R) -> Result<BlockRecord, NetError> {
    let n_parents = read_u64(r)? as usize;
    if n_parents > 4_096 {
        return Err(NetError::Decode("parent count too large".into()));
    }
    let mut parents = Vec::with_capacity(n_parents);
    for _ in 0..n_parents {
        let mut id = [0u8; 32];
        r.read_exact(&mut id).map_err(io)?;
        parents.push(BlockId::from_bytes(id));
    }
    let mut work = [0u8; 16];
    r.read_exact(&mut work).map_err(io)?;
    let mut timestamp_ms = [0u8; 8];
    r.read_exact(&mut timestamp_ms).map_err(io)?;
    let mut nonce = [0u8; 8];
    r.read_exact(&mut nonce).map_err(io)?;
    let payload_len = read_u64(r)? as usize;
    if payload_len > 16 * 1024 * 1024 {
        return Err(NetError::Decode("payload too large".into()));
    }
    let mut payload = vec![0u8; payload_len];
    r.read_exact(&mut payload).map_err(io)?;
    let txs = decode_block_payload(&payload).map_err(|e| NetError::Decode(e.to_string()))?;
    Ok(BlockRecord {
        parents,
        work: u128::from_le_bytes(work),
        timestamp_ms: u64::from_le_bytes(timestamp_ms),
        nonce: u64::from_le_bytes(nonce),
        txs,
    })
}

/// Read a framed dump (count + records) from any blocking reader — a socket,
/// or a `Cursor` over bytes in tests. Bounds are checked per field so a
/// hostile length cannot balloon memory before data arrives.
pub fn read_records_from<R: Read>(r: &mut R) -> Result<Vec<BlockRecord>, NetError> {
    let count = read_u64(r)? as usize;
    if count > 1_000_000 {
        return Err(NetError::Decode("count too large".into()));
    }
    let mut records = Vec::with_capacity(count);
    for _ in 0..count {
        records.push(read_record_from(r)?);
    }
    Ok(records)
}

/// Why a sync failed.
#[derive(Debug)]
pub enum NetError {
    /// A socket read/write failed.
    Io(String),
    /// The peer's bytes could not be decoded into block records.
    Decode(String),
    /// Applying a received block failed.
    Apply(String),
}

impl core::fmt::Display for NetError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            NetError::Io(e) => write!(f, "io: {e}"),
            NetError::Decode(e) => write!(f, "decode: {e}"),
            NetError::Apply(e) => write!(f, "apply: {e}"),
        }
    }
}

impl std::error::Error for NetError {}

fn io(e: std::io::Error) -> NetError {
    NetError::Io(e.to_string())
}

/// Wire encoding of block records: count, then per record — parents
/// (count + 32-byte ids), work (u128), timestamp (u64), nonce (u64), and the
/// block payload (length-prefixed, the same encoding a block carries).
pub fn encode_records(records: &[BlockRecord]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&(records.len() as u64).to_le_bytes());
    for record in records {
        encode_record(record, &mut buf);
    }
    buf
}

pub(crate) fn encode_record(record: &BlockRecord, buf: &mut Vec<u8>) {
    buf.extend_from_slice(&(record.parents.len() as u64).to_le_bytes());
    for parent in &record.parents {
        buf.extend_from_slice(parent.as_bytes());
    }
    buf.extend_from_slice(&record.work.to_le_bytes());
    buf.extend_from_slice(&record.timestamp_ms.to_le_bytes());
    buf.extend_from_slice(&record.nonce.to_le_bytes());
    let payload = encode_block_payload(&record.txs);
    buf.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    buf.extend_from_slice(&payload);
}

fn decode_records(bytes: &[u8]) -> Result<Vec<BlockRecord>, NetError> {
    let mut r = Cursor { buf: bytes, pos: 0 };
    // Each record is at least 8 (parents len) + 16 (work) + 8 (timestamp) +
    // 8 (nonce) + 8 (payload len) = 48 bytes.
    let count = r.read_count(48)?;
    let mut records = Vec::with_capacity(count);
    for _ in 0..count {
        records.push(decode_record(&mut r)?);
    }
    if r.pos != bytes.len() {
        return Err(NetError::Decode("trailing bytes".into()));
    }
    Ok(records)
}

fn decode_record(r: &mut Cursor<'_>) -> Result<BlockRecord, NetError> {
    let n_parents = r.read_count(32)?;
    let mut parents = Vec::with_capacity(n_parents);
    for _ in 0..n_parents {
        parents.push(BlockId::from_bytes(r.read_array::<32>()?));
    }
    let work = u128::from_le_bytes(r.read_array::<16>()?);
    let timestamp_ms = u64::from_le_bytes(r.read_array::<8>()?);
    let nonce = u64::from_le_bytes(r.read_array::<8>()?);
    let payload_len = r.read_count(1)?;
    let payload = r.read_slice(payload_len)?;
    let txs = decode_block_payload(payload).map_err(|e| NetError::Decode(e.to_string()))?;
    Ok(BlockRecord {
        parents,
        work,
        timestamp_ms,
        nonce,
        txs,
    })
}

pub(crate) fn decode_one_record(bytes: &[u8]) -> Result<BlockRecord, NetError> {
    let mut r = Cursor { buf: bytes, pos: 0 };
    let rec = decode_record(&mut r)?;
    if r.pos != bytes.len() {
        return Err(NetError::Decode("trailing bytes".into()));
    }
    Ok(rec)
}

/// A minimal bounds-checked reader.
struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N], NetError> {
        if self.remaining() < N {
            return Err(NetError::Decode("unexpected end".into()));
        }
        let mut out = [0u8; N];
        out.copy_from_slice(&self.buf[self.pos..self.pos + N]);
        self.pos += N;
        Ok(out)
    }

    fn read_slice(&mut self, len: usize) -> Result<&'a [u8], NetError> {
        if self.remaining() < len {
            return Err(NetError::Decode("unexpected end".into()));
        }
        let out = &self.buf[self.pos..self.pos + len];
        self.pos += len;
        Ok(out)
    }

    fn read_count(&mut self, min_element_bytes: usize) -> Result<usize, NetError> {
        let n = u64::from_le_bytes(self.read_array::<8>()?) as usize;
        if min_element_bytes > 0 && n > self.remaining() / min_element_bytes {
            return Err(NetError::Decode("count too large".into()));
        }
        Ok(n)
    }
}

/// Length-prefixed frame for streaming reads/writes over TCP.
fn write_frame<W: Write>(w: &mut W, bytes: &[u8]) -> Result<(), NetError> {
    w.write_all(&(bytes.len() as u32).to_le_bytes())
        .map_err(io)?;
    w.write_all(bytes).map_err(io)?;
    w.flush().map_err(io)
}

fn read_frame<R: Read>(r: &mut R, max_bytes: usize) -> Result<Vec<u8>, NetError> {
    let mut len_buf = [0u8; 4];
    r.read_exact(&mut len_buf).map_err(io)?;
    let len = u32::from_le_bytes(len_buf) as usize;
    if len > max_bytes {
        return Err(NetError::Decode("frame too large".into()));
    }
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf).map_err(io)?;
    Ok(buf)
}

/// Sync protocol message tags.
const TAG_INVENTORY: u8 = 0x10; // Vec<BlockId> (sorted, deduped)
const TAG_HEADERS: u8 = 0x11; // Vec<BlockHeader>
const TAG_GETHEADERS: u8 = 0x12; // Vec<BlockId> (ids whose headers we want)
const TAG_GETBODIES: u8 = 0x13; // Vec<BlockId> (ids whose bodies we want)
const TAG_BODIES: u8 = 0x14; // Vec<BlockRecord>

const MAX_INVENTORY_IDS: usize = 200_000; // 6.4 MB max
const MAX_HEADERS: usize = 10_000; // ~3 MB max
const MAX_GETBODIES: usize = 10_000;
const MAX_BODIES: usize = 10_000;
const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024; // 16 MB

/// Encode an inventory message (sorted, deduped block ids).
pub fn encode_inventory(ids: &[BlockId]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1 + 8 + ids.len() * 32);
    buf.push(TAG_INVENTORY);
    buf.extend_from_slice(&(ids.len() as u64).to_le_bytes());
    for id in ids {
        buf.extend_from_slice(id.as_bytes());
    }
    buf
}

/// Decode an inventory message into a sorted, deduped vec.
pub fn decode_inventory(bytes: &[u8]) -> Result<Vec<BlockId>, NetError> {
    if bytes.is_empty() || bytes[0] != TAG_INVENTORY {
        return Err(NetError::Decode("not an inventory frame".into()));
    }
    let mut r = Cursor {
        buf: &bytes[1..],
        pos: 0,
    };
    let count = r.read_count(32)?;
    if count > MAX_INVENTORY_IDS {
        return Err(NetError::Decode("inventory count too large".into()));
    }
    let mut ids = Vec::with_capacity(count);
    for _ in 0..count {
        ids.push(BlockId::from_bytes(r.read_array::<32>()?));
    }
    if r.pos != r.buf.len() {
        return Err(NetError::Decode("trailing bytes".into()));
    }
    Ok(ids)
}

/// Encode a headers message.
pub fn encode_headers(headers: &[BlockHeader]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1 + 8 + headers.len() * 80);
    buf.push(TAG_HEADERS);
    buf.extend_from_slice(&(headers.len() as u64).to_le_bytes());
    for h in headers {
        encode_header(h, &mut buf);
    }
    buf
}

fn encode_header(h: &BlockHeader, buf: &mut Vec<u8>) {
    buf.extend_from_slice(h.id.as_bytes());
    buf.extend_from_slice(&(h.parents.len() as u64).to_le_bytes());
    for p in &h.parents {
        buf.extend_from_slice(p.as_bytes());
    }
    buf.extend_from_slice(&h.work.to_le_bytes());
    buf.extend_from_slice(&h.timestamp_ms.to_le_bytes());
    buf.extend_from_slice(&h.nonce.to_le_bytes());
    buf.extend_from_slice(&h.payload_hash);
    buf.extend_from_slice(&h.payload_len.to_le_bytes());
}

/// Decode a headers message.
pub fn decode_headers(bytes: &[u8]) -> Result<Vec<BlockHeader>, NetError> {
    if bytes.is_empty() || bytes[0] != TAG_HEADERS {
        return Err(NetError::Decode("not a headers frame".into()));
    }
    let mut r = Cursor {
        buf: &bytes[1..],
        pos: 0,
    };
    let count = r.read_count(1)?; // min_element_bytes=1 is fine
    if count > MAX_HEADERS {
        return Err(NetError::Decode("headers count too large".into()));
    }
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        let id = BlockId::from_bytes(r.read_array::<32>()?);
        let n_parents = r.read_count(32)?;
        let mut parents = Vec::with_capacity(n_parents);
        for _ in 0..n_parents {
            parents.push(BlockId::from_bytes(r.read_array::<32>()?));
        }
        let work = u128::from_le_bytes(r.read_array::<16>()?);
        let timestamp_ms = u64::from_le_bytes(r.read_array::<8>()?);
        let nonce = u64::from_le_bytes(r.read_array::<8>()?);
        let payload_hash = r.read_array::<32>()?;
        let payload_len = u64::from_le_bytes(r.read_array::<8>()?);
        out.push(BlockHeader {
            id,
            parents,
            work,
            timestamp_ms,
            nonce,
            payload_hash,
            payload_len,
        });
    }
    if r.pos != r.buf.len() {
        return Err(NetError::Decode("trailing bytes".into()));
    }
    Ok(out)
}

/// Encode a getheaders message (ids we want headers for).
pub fn encode_getheaders(ids: &[BlockId]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1 + 8 + ids.len() * 32);
    buf.push(TAG_GETHEADERS);
    buf.extend_from_slice(&(ids.len() as u64).to_le_bytes());
    for id in ids {
        buf.extend_from_slice(id.as_bytes());
    }
    buf
}

/// Decode a getheaders message.
pub fn decode_getheaders(bytes: &[u8]) -> Result<Vec<BlockId>, NetError> {
    if bytes.is_empty() || bytes[0] != TAG_GETHEADERS {
        return Err(NetError::Decode("not a getheaders frame".into()));
    }
    let mut r = Cursor {
        buf: &bytes[1..],
        pos: 0,
    };
    let count = r.read_count(32)?;
    if count > MAX_GETBODIES {
        return Err(NetError::Decode("getheaders count too large".into()));
    }
    let mut ids = Vec::with_capacity(count);
    for _ in 0..count {
        ids.push(BlockId::from_bytes(r.read_array::<32>()?));
    }
    if r.pos != r.buf.len() {
        return Err(NetError::Decode("trailing bytes".into()));
    }
    Ok(ids)
}

/// Encode a getbodies message.
pub fn encode_getbodies(ids: &[BlockId]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1 + 8 + ids.len() * 32);
    buf.push(TAG_GETBODIES);
    buf.extend_from_slice(&(ids.len() as u64).to_le_bytes());
    for id in ids {
        buf.extend_from_slice(id.as_bytes());
    }
    buf
}

/// Decode a getbodies message.
pub fn decode_getbodies(bytes: &[u8]) -> Result<Vec<BlockId>, NetError> {
    if bytes.is_empty() || bytes[0] != TAG_GETBODIES {
        return Err(NetError::Decode("not a getbodies frame".into()));
    }
    let mut r = Cursor {
        buf: &bytes[1..],
        pos: 0,
    };
    let count = r.read_count(32)?;
    if count > MAX_GETBODIES {
        return Err(NetError::Decode("getbodies count too large".into()));
    }
    let mut ids = Vec::with_capacity(count);
    for _ in 0..count {
        ids.push(BlockId::from_bytes(r.read_array::<32>()?));
    }
    if r.pos != r.buf.len() {
        return Err(NetError::Decode("trailing bytes".into()));
    }
    Ok(ids)
}

/// Encode a bodies message.
pub fn encode_bodies(records: &[BlockRecord]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1 + 8);
    buf.push(TAG_BODIES);
    buf.extend_from_slice(&(records.len() as u64).to_le_bytes());
    for rec in records {
        encode_record(rec, &mut buf);
    }
    buf
}

/// Decode a bodies message.
pub fn decode_bodies(bytes: &[u8]) -> Result<Vec<BlockRecord>, NetError> {
    if bytes.is_empty() || bytes[0] != TAG_BODIES {
        return Err(NetError::Decode("not a bodies frame".into()));
    }
    let mut r = Cursor {
        buf: &bytes[1..],
        pos: 0,
    };
    let count = r.read_count(48)?;
    if count > MAX_BODIES {
        return Err(NetError::Decode("bodies count too large".into()));
    }
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        out.push(decode_record(&mut r)?);
    }
    if r.pos != r.buf.len() {
        return Err(NetError::Decode("trailing bytes".into()));
    }
    Ok(out)
}

/// Result of a headers-first sync exchange.
#[derive(Debug, Default)]
pub struct SyncStats {
    pub headers_received: usize,
    pub bodies_requested: usize,
    pub bodies_received: usize,
    pub bodies_applied: usize,
    pub errors: usize,
}

/// Client-side headers-first sync against a peer at `addr`.
/// Steps:
/// 1. Exchange inventories (our inventory, peer's inventory).
/// 2. Compute missing ids = peer_ids \ our_ids.
/// 3. Request headers for missing ids (in chunks if large).
/// 4. For each batch of headers, request bodies by id and apply them in topo order.
/// Returns stats on success.
pub fn sync_headers_first(
    addr: &str,
    node: &mut Node,
    timeout: Duration,
) -> Result<SyncStats, NetError> {
    let mut socks: Vec<_> = addr.to_socket_addrs().map_err(io)?.collect();
    if socks.is_empty() {
        return Err(NetError::Io("no address".into()));
    }
    socks.sort_by_key(|s| if s.is_ipv4() { 0u8 } else { 1 });

    let mut last = NetError::Io("no address".into());
    for sock in socks {
        match TcpStream::connect_timeout(&sock, timeout) {
            Ok(mut stream) => {
                stream.set_read_timeout(Some(timeout)).map_err(io)?;
                stream.set_write_timeout(Some(timeout)).map_err(io)?;

                // Step 1: exchange inventories
                let our_inv = encode_inventory(&node.inventory());
                write_frame(&mut stream, &our_inv)?;
                let peer_inv_bytes = read_frame(&mut stream, MAX_FRAME_BYTES)?;
                let peer_inv = decode_inventory(&peer_inv_bytes)?;

                // Step 2: compute missing
                let our_set: std::collections::BTreeSet<BlockId> =
                    node.inventory().into_iter().collect();
                let missing: Vec<BlockId> = peer_inv
                    .into_iter()
                    .filter(|id| !our_set.contains(id))
                    .collect();
                if missing.is_empty() {
                    return Ok(SyncStats::default());
                }

                // Step 3: request headers for all missing (one request, peer sends in topo order)
                let get_headers = encode_getheaders(&missing);
                write_frame(&mut stream, &get_headers)?;

                // Step 4: receive headers
                let headers_bytes = read_frame(&mut stream, MAX_FRAME_BYTES)?;
                let headers = decode_headers(&headers_bytes)?;

                // Step 5: request and apply bodies in chunks
                let mut stats = SyncStats {
                    headers_received: headers.len(),
                    bodies_requested: headers.len(),
                    ..Default::default()
                };
                for chunk in headers.chunks(MAX_BODIES) {
                    let ids: Vec<BlockId> = chunk.iter().map(|h| h.id).collect();
                    let req = encode_getbodies(&ids);
                    write_frame(&mut stream, &req)?;
                    let bodies_bytes = read_frame(&mut stream, MAX_FRAME_BYTES)?;
                    let bodies = decode_bodies(&bodies_bytes)?;
                    stats.bodies_received += bodies.len();
                    for (i, body) in bodies.into_iter().enumerate() {
                        let header = &chunk[i];
                        // Verify body matches header before applying
                        if Node::verify_header_body(header, &body).is_none() {
                            stats.errors += 1;
                            continue;
                        }
                        match node.receive_block(body) {
                            Ok(_) => stats.bodies_applied += 1,
                            Err(_) => stats.errors += 1,
                        }
                    }
                }
                return Ok(stats);
            }
            Err(e) => last = io(e),
        }
    }
    Err(last)
}

/// Server-side: run a headers-first sync exchange on an accepted stream.
/// Reads our inventory, writes peer's inventory, then serves headers/bodies on demand.
/// Returns when the peer closes the connection or on error.
pub fn serve_headers_first(
    stream: &mut TcpStream,
    node: &mut Node,
    timeout: Duration,
) -> Result<(), NetError> {
    stream.set_read_timeout(Some(timeout)).map_err(io)?;
    stream.set_write_timeout(Some(timeout)).map_err(io)?;

    // Step 1: read client inventory, write our inventory
    let client_inv_bytes = read_frame(stream, MAX_FRAME_BYTES)?;
    let client_inv = decode_inventory(&client_inv_bytes)?;
    let our_inv = encode_inventory(&node.inventory());
    write_frame(stream, &our_inv)?;

    // Step 2: read get-headers (client sends ids it wants headers for)
    let get_headers_bytes = read_frame(stream, MAX_FRAME_BYTES)?;
    let want_ids = decode_getheaders(&get_headers_bytes)?;

    // Step 3: respond with headers for those ids (in the order client sent — client knows topo order)
    let headers = node.headers_for(&want_ids);
    let headers_frame = encode_headers(&headers);
    write_frame(stream, &headers_frame)?;

    // Step 4: loop: read getbodies, write bodies until EOF or error
    loop {
        let req_bytes = match read_frame(stream, MAX_FRAME_BYTES) {
            Ok(b) => b,
            Err(NetError::Io(_)) => break, // peer closed
            Err(e) => return Err(e),
        };
        let want = decode_getbodies(&req_bytes)?;
        let records: Vec<BlockRecord> =
            want.iter().filter_map(|id| node.block_record(id)).collect();
        let bodies_frame = encode_bodies(&records);
        if let Err(e) = write_frame(stream, &bodies_frame) {
            // Peer may have closed; not an error.
            if matches!(e, NetError::Io(_)) {
                break;
            }
            return Err(e);
        }
    }
    Ok(())
}

/// Backward-compatible full-dump exchange (used by explorer loop).
/// Performs a framed bidirectional exchange: reads peer's records, applies them,
/// then sends our pre-exchange snapshot back.
pub fn exchange_full_dump(
    stream: &mut TcpStream,
    node: &mut Node,
    timeout: Duration,
) -> Result<usize, NetError> {
    stream.set_nonblocking(false).map_err(io)?;
    stream.set_read_timeout(Some(timeout)).map_err(io)?;
    stream.set_write_timeout(Some(timeout)).map_err(io)?;
    let mine = encode_records(&node.export());
    stream.write_all(&mine).map_err(io)?;
    stream.flush().map_err(io)?;
    match read_records_from(stream) {
        Ok(recs) => apply_decoded(recs, node),
        Err(NetError::Io(_)) => Ok(0),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn two_records() -> Vec<BlockRecord> {
        let mut node = Node::new();
        node.genesis(3, 100, 100, 1).expect("genesis");
        node.send_to(1, 10, Node::address(2)).expect("send");
        node.export()
    }

    #[test]
    fn framed_reader_roundtrips_encoded_records() {
        let records = two_records();
        let bytes = encode_records(&records);
        let mut cursor = Cursor::new(bytes);
        let read_back = read_records_from(&mut cursor).expect("decode");
        assert_eq!(read_back.len(), records.len());
        for (a, b) in read_back.iter().zip(&records) {
            assert_eq!(a.parents, b.parents);
            assert_eq!(a.work, b.work);
            assert_eq!(a.timestamp_ms, b.timestamp_ms);
            assert_eq!(a.nonce, b.nonce);
            assert_eq!(a.txs.len(), b.txs.len());
        }
    }

    #[test]
    fn empty_frame_reads_as_no_records() {
        let mut cursor = Cursor::new(0u64.to_le_bytes());
        assert!(read_records_from(&mut cursor).expect("empty").is_empty());
    }

    #[test]
    fn truncated_frame_is_an_error_not_a_hang() {
        let bytes = encode_records(&two_records());
        let mut cursor = Cursor::new(bytes[..bytes.len() - 1].to_vec());
        assert!(read_records_from(&mut cursor).is_err());
    }

    #[test]
    fn absurd_counts_are_rejected_before_allocation() {
        // Count claims 2^40 records; the bound check must fire on the count
        // alone (the "buffer" holds nothing else).
        let mut frame = Vec::new();
        frame.extend_from_slice(&u64::MAX.to_le_bytes());
        let mut cursor = Cursor::new(frame);
        let err = read_records_from(&mut cursor).expect_err("count too large");
        assert!(matches!(err, NetError::Decode(_)));
    }

    fn genesis_node() -> Node {
        let mut node = Node::new();
        node.genesis(3, 1000, 1000, 1).unwrap();
        node
    }

    #[test]
    fn header_roundtrip() {
        let mut node = genesis_node();
        node.send(1, 400, 2).unwrap();
        let headers = node.export_headers();
        assert!(!headers.is_empty());
        let bytes = encode_headers(&headers);
        let decoded = decode_headers(&bytes).unwrap();
        assert_eq!(decoded.len(), headers.len());
        for (a, b) in decoded.iter().zip(&headers) {
            assert_eq!(a.id, b.id);
            assert_eq!(a.parents, b.parents);
            assert_eq!(a.work, b.work);
            assert_eq!(a.timestamp_ms, b.timestamp_ms);
            assert_eq!(a.nonce, b.nonce);
            assert_eq!(a.payload_hash, b.payload_hash);
            assert_eq!(a.payload_len, b.payload_len);
        }
    }

    #[test]
    fn inventory_roundtrip() {
        let mut node = genesis_node();
        node.send(1, 400, 2).unwrap();
        let inv = node.inventory();
        assert!(!inv.is_empty());
        let bytes = encode_inventory(&inv);
        let decoded = decode_inventory(&bytes).unwrap();
        assert_eq!(decoded, inv);
    }

    #[test]
    fn getheaders_roundtrip() {
        let mut node = genesis_node();
        node.send(1, 400, 2).unwrap();
        let ids = node.inventory();
        let bytes = encode_getheaders(&ids);
        let decoded = decode_getheaders(&bytes).unwrap();
        assert_eq!(decoded, ids);
    }

    #[test]
    fn getbodies_roundtrip() {
        let mut node = genesis_node();
        node.send(1, 400, 2).unwrap();
        let ids = node.inventory();
        let bytes = encode_getbodies(&ids);
        let decoded = decode_getbodies(&bytes).unwrap();
        assert_eq!(decoded, ids);
    }

    #[test]
    fn bodies_roundtrip() {
        let mut node = genesis_node();
        node.send(1, 400, 2).unwrap();
        let records = node.export();
        assert!(!records.is_empty());
        let bytes = encode_bodies(&records);
        let decoded = decode_bodies(&bytes).unwrap();
        assert_eq!(decoded.len(), records.len());
    }

    #[test]
    fn header_body_verify_matches() {
        let mut node = genesis_node();
        node.send(1, 400, 2).unwrap();
        let headers = node.export_headers();
        let records = node.export();
        for (h, r) in headers.iter().zip(&records) {
            assert!(Node::verify_header_body(h, r).is_some());
        }
    }

    #[test]
    fn header_body_verify_rejects_mismatch() {
        let mut node = genesis_node();
        node.send(1, 400, 2).unwrap();
        let headers = node.export_headers();
        let records = node.export();
        // Mismatch: use header from one block with body from another
        if headers.len() >= 2 && records.len() >= 2 {
            assert!(Node::verify_header_body(&headers[0], &records[1]).is_none());
        }
    }
}
