//! Incremental on-disk ledger: an append-only replay log.
//!
//! [`Ledger::write_snapshot`] rewrites the *whole* DAG every time. [`LedgerStore`]
//! writes a short header once, then **appends** each subsequent block as a
//! length-prefixed record. Loading replays the log through [`Ledger::insert`] —
//! derived consensus and UTXO state is never trusted from disk, same as the
//! snapshot. The file is a streaming log, not mmap.

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::Path;

use kovanica_dag::{decode_block, encode_block, Block, SnapshotError};

use crate::ledger::{Ledger, LedgerError, LedgerSnapshotError, HalvingSchedule, DEFAULT_HALVING_ERA};

/// Magic prefix identifying a Kovanica ledger log (`"KVLF"`).
const MAGIC: [u8; 4] = *b"KVLF";
/// Log format version. Bump on any incompatible framing change.
const VERSION: u16 = 2;
/// Refuse a single on-disk record larger than this.
const MAX_RECORD: usize = 16 * 1024 * 1024;

/// An open append-only ledger log.
pub struct LedgerStore {
    file: File,
}

/// Why a log could not be created, opened, or appended.
#[derive(Debug)]
pub enum StoreError {
    /// A filesystem read or write failed.
    Io(String),
    /// The file did not start with the expected magic.
    BadMagic,
    /// The log version is not supported by this build.
    UnsupportedVersion(u16),
    /// The log ended in the middle of a record (or had no genesis).
    Truncated,
    /// A stored block could not be decoded.
    Block(SnapshotError),
    /// Replaying the log into a ledger failed.
    Replay(LedgerSnapshotError),
}

impl core::fmt::Display for StoreError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            StoreError::Io(e) => write!(f, "io: {e}"),
            StoreError::BadMagic => f.write_str("not a kovanica ledger log"),
            StoreError::UnsupportedVersion(v) => write!(f, "unsupported log version {v}"),
            StoreError::Truncated => f.write_str("truncated ledger log"),
            StoreError::Block(e) => write!(f, "block: {e}"),
            StoreError::Replay(e) => write!(f, "replay: {e}"),
        }
    }
}

impl std::error::Error for StoreError {}

impl From<io::Error> for StoreError {
    fn from(e: io::Error) -> Self {
        StoreError::Io(e.to_string())
    }
}

impl LedgerStore {
    /// Create (or replace) a log at `path` and write every block currently in
    /// `ledger`, genesis first. Subsequent [`append`](Self::append) calls add
    /// only new blocks.
    pub fn create(path: impl AsRef<Path>, ledger: &Ledger) -> Result<Self, StoreError> {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .truncate(true)
            .open(path)?;
        file.write_all(&MAGIC)?;
        file.write_all(&VERSION.to_le_bytes())?;
        file.write_all(&ledger.schedule().genesis_subsidy.to_le_bytes())?;
        file.write_all(&ledger.schedule().halving_era.to_le_bytes())?;
        file.write_all(&ledger.dag().k().to_le_bytes())?;
        for id in ledger.dag().linearize() {
            let block = ledger.dag().block(&id).expect("linearized id is present");
            write_record(&mut file, block)?;
        }
        file.flush()?;
        Ok(Self { file })
    }

    /// Open an existing log and replay it into a [`Ledger`].
    pub fn open(path: impl AsRef<Path>) -> Result<(Self, Ledger), StoreError> {
        let mut file = OpenOptions::new().read(true).write(true).open(path)?;
        let mut magic = [0u8; 4];
        file.read_exact(&mut magic).map_err(map_header_eof)?;
        if magic != MAGIC {
            return Err(StoreError::BadMagic);
        }
        let mut ver = [0u8; 2];
        file.read_exact(&mut ver).map_err(map_header_eof)?;
        let version = u16::from_le_bytes(ver);
        if version > VERSION {
            return Err(StoreError::UnsupportedVersion(version));
        }
        let mut sub = [0u8; 8];
        file.read_exact(&mut sub).map_err(map_header_eof)?;
        let subsidy = u64::from_le_bytes(sub);
        let halving_era = if version >= 2 {
            let mut era_buf = [0u8; 8];
            file.read_exact(&mut era_buf).map_err(map_header_eof)?;
            u64::from_le_bytes(era_buf)
        } else {
            DEFAULT_HALVING_ERA
        };
        let mut kbuf = [0u8; 2];
        file.read_exact(&mut kbuf).map_err(map_header_eof)?;
        let k = u16::from_le_bytes(kbuf);

        let genesis = read_record(&mut file)?.ok_or(StoreError::Truncated)?;
        let genesis_txs = kovanica_dag_payload(&genesis).map_err(StoreError::Replay)?;
        let schedule = HalvingSchedule::new(subsidy, halving_era);
        let mut ledger = Ledger::new(k, schedule, &genesis_txs)
            .map_err(|e| StoreError::Replay(map_genesis(e)))?;
        while let Some(block) = read_record(&mut file)? {
            let txs = kovanica_dag_payload(&block).map_err(StoreError::Replay)?;
            ledger
                .insert(
                    block.parents().to_vec(),
                    block.work(),
                    block.timestamp_ms(),
                    block.nonce(),
                    &txs,
                )
                .map_err(|e| StoreError::Replay(LedgerSnapshotError::Rebuild(e)))?;
        }
        Ok((Self { file }, ledger))
    }

    /// Append one block (already in the ledger) to the log and flush.
    pub fn append(&mut self, block: &Block) -> Result<(), StoreError> {
        write_record(&mut self.file, block)?;
        self.file.flush()?;
        Ok(())
    }
}

fn map_header_eof(e: io::Error) -> StoreError {
    if e.kind() == io::ErrorKind::UnexpectedEof {
        StoreError::Truncated
    } else {
        StoreError::Io(e.to_string())
    }
}

fn map_genesis(e: LedgerError) -> LedgerSnapshotError {
    LedgerSnapshotError::Genesis(e)
}

fn kovanica_dag_payload(block: &Block) -> Result<Vec<crate::tx::Transaction>, LedgerSnapshotError> {
    crate::tx::decode_block_payload(block.payload()).map_err(LedgerSnapshotError::Payload)
}

fn write_record(file: &mut File, block: &Block) -> Result<(), StoreError> {
    let mut body = Vec::new();
    encode_block(block, &mut body);
    if body.len() > MAX_RECORD {
        return Err(StoreError::Io("record too large".into()));
    }
    file.write_all(&(body.len() as u64).to_le_bytes())?;
    file.write_all(&body)?;
    Ok(())
}

fn read_record(file: &mut File) -> Result<Option<Block>, StoreError> {
    let mut len_buf = [0u8; 8];
    match file.read_exact(&mut len_buf) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    }
    let len = u64::from_le_bytes(len_buf) as usize;
    if len == 0 || len > MAX_RECORD {
        return Err(StoreError::Truncated);
    }
    let mut body = vec![0u8; len];
    file.read_exact(&mut body).map_err(map_header_eof)?;
    let block = decode_block(&body).map_err(StoreError::Block)?;
    Ok(Some(block))
}
