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

use crate::node::{BlockRecord, Node};

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
}
