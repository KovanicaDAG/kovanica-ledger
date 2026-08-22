//! Structural (context-free) validation of block payloads, and the
//! [`kovanica_dag::BlockValidator`] that runs it at insert time.
//!
//! These checks reject blocks that are malformed or structurally invalid
//! *regardless of ledger state* — they never consult the [`UtxoSet`]. They are
//! exactly the subset of the ledger's rules (see [`crate::ledger`]) that depend
//! only on a transaction's own contents:
//!
//! * the payload decodes into transactions;
//! * a coinbase (input-less) transaction appears only as the first transaction;
//! * every non-coinbase transaction has at least one output;
//! * no transaction spends the same outpoint twice;
//! * every output value is non-zero;
//! * a transaction's output values do not overflow `u64`.
//!
//! The **stateful** rules — an input exists and is unspent, its signature
//! verifies against the spent output's owner, value is conserved, and the
//! coinbase claims at most `subsidy + fees` — need the UTXO state at the block's
//! position and therefore remain in [`crate::ledger`], enforced when the DAG is
//! applied. A block that passes structural validation at insert can still be
//! rejected there. Full stateful validation at insert awaits per-block UTXO
//! state (the selected-parent UTXO set plus mergeset diffs), a later slice.
//!
//! Install [`TxStructureValidator`] on a DAG to have these checks run on every
//! insert:
//!
//! ```
//! use kovanica_dag::{Block, Dag};
//! use kovanica_state::{encode_block_payload, Transaction, TxOutput, KeyPair, TxStructureValidator};
//!
//! let alice = KeyPair::from_u64(1);
//! let genesis = Block::genesis(1, 0, 0, b"genesis".to_vec());
//! let mut dag = Dag::with_validator(3, genesis, Box::new(TxStructureValidator));
//! let genesis_id = dag.genesis();
//!
//! // A well-formed coinbase-carrying block is accepted.
//! let cb = Transaction::coinbase(vec![TxOutput::new(50, alice.address())], b"h1".to_vec());
//! assert!(dag.insert(Block::new(vec![genesis_id], 1, 1, 0, encode_block_payload(&[cb]))).is_ok());
//!
//! // A block whose payload is not valid transaction encoding is rejected at insert.
//! assert!(dag.insert(Block::new(vec![genesis_id], 1, 1, 0, b"not-transactions".to_vec())).is_err());
//! ```
//!
//! [`UtxoSet`]: crate::utxo::UtxoSet

use core::fmt;
use std::collections::HashSet;

use kovanica_dag::{Block, BlockValidator, Dag};

use crate::tx::{decode_block_payload, DecodeError, OutPoint, Transaction};

/// Maximum serialized transaction size in bytes (100 KB).
pub const MAX_TX_SIZE: usize = 100_000;
/// Maximum serialized block payload size in bytes (1 MB).
pub const MAX_BLOCK_PAYLOAD_SIZE: usize = 1_000_000;
/// Maximum number of transactions per block.
pub const MAX_TXS_PER_BLOCK: usize = 1000;

/// Why a block's payload failed structural (context-free) validation.
///
/// Every variant names the offending transaction by its index within the block,
/// so the failure can be pinpointed without the UTXO state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BlockValidationError {
    /// The payload could not be decoded into a list of transactions.
    Payload(DecodeError),
    /// A coinbase (input-less) transaction appeared at a non-first position.
    MisplacedCoinbase { tx_index: usize },
    /// A non-coinbase transaction has no outputs.
    NoOutputs { tx_index: usize },
    /// A transaction spends the same outpoint more than once.
    DuplicateInput { tx_index: usize, outpoint: OutPoint },
    /// A transaction has an output with zero value.
    ZeroValueOutput {
        tx_index: usize,
        output_index: usize,
    },
    /// A transaction's output values overflow `u64` when summed.
    ValueOverflow { tx_index: usize },
    /// A transaction exceeds the maximum allowed size.
    TxTooLarge { tx_index: usize, size: usize },
    /// Block payload exceeds the maximum allowed size.
    BlockPayloadTooLarge { size: usize },
    /// Block contains too many transactions.
    TooManyTransactions { count: usize },
}

impl fmt::Display for BlockValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BlockValidationError::Payload(e) => write!(f, "payload decode: {e}"),
            BlockValidationError::MisplacedCoinbase { tx_index } => {
                write!(f, "misplaced coinbase at tx {tx_index}")
            }
            BlockValidationError::NoOutputs { tx_index } => {
                write!(f, "tx {tx_index} has no outputs")
            }
            BlockValidationError::DuplicateInput { tx_index, outpoint } => {
                write!(f, "tx {tx_index} spends {outpoint:?} twice")
            }
            BlockValidationError::ZeroValueOutput {
                tx_index,
                output_index,
            } => write!(f, "tx {tx_index} output {output_index} has zero value"),
            BlockValidationError::ValueOverflow { tx_index } => {
                write!(f, "tx {tx_index} output values overflow")
            }
            BlockValidationError::TxTooLarge { tx_index, size } => {
                write!(f, "tx {tx_index} size {size} exceeds limit {MAX_TX_SIZE}")
            }
            BlockValidationError::BlockPayloadTooLarge { size } => {
                write!(
                    f,
                    "block payload size {size} exceeds limit {MAX_BLOCK_PAYLOAD_SIZE}"
                )
            }
            BlockValidationError::TooManyTransactions { count } => {
                write!(
                    f,
                    "block has {count} transactions, limit is {MAX_TXS_PER_BLOCK}"
                )
            }
        }
    }
}

impl std::error::Error for BlockValidationError {}

/// Run the structural (context-free) checks over a block payload. This is the
/// standalone form of what [`TxStructureValidator`] runs at insert time.
pub fn validate_block_payload(payload: &[u8]) -> Result<(), BlockValidationError> {
    if payload.len() > MAX_BLOCK_PAYLOAD_SIZE {
        return Err(BlockValidationError::BlockPayloadTooLarge {
            size: payload.len(),
        });
    }
    let txs = decode_block_payload(payload).map_err(BlockValidationError::Payload)?;
    if txs.len() > MAX_TXS_PER_BLOCK {
        return Err(BlockValidationError::TooManyTransactions { count: txs.len() });
    }
    for (i, tx) in txs.iter().enumerate() {
        validate_tx_structure(i, tx)?;
    }
    Ok(())
}

/// Structural checks for a single transaction at position `index` in its block.
fn validate_tx_structure(index: usize, tx: &Transaction) -> Result<(), BlockValidationError> {
    let tx_size = tx.encode().len();
    if tx_size > MAX_TX_SIZE {
        return Err(BlockValidationError::TxTooLarge {
            tx_index: index,
            size: tx_size,
        });
    }

    if tx.is_coinbase() {
        // A coinbase (no inputs) may only be the first transaction. This also
        // rejects a second coinbase, which would land at index != 0.
        if index != 0 {
            return Err(BlockValidationError::MisplacedCoinbase { tx_index: index });
        }
    } else if tx.outputs().is_empty() {
        // A spend that creates nothing burns its whole input to fees for no
        // reason; treat an empty-output non-coinbase as malformed.
        return Err(BlockValidationError::NoOutputs { tx_index: index });
    }

    let mut seen: HashSet<OutPoint> = HashSet::with_capacity(tx.inputs().len());
    for input in tx.inputs() {
        if !seen.insert(input.outpoint) {
            return Err(BlockValidationError::DuplicateInput {
                tx_index: index,
                outpoint: input.outpoint,
            });
        }
    }

    let mut sum: u64 = 0;
    for (j, output) in tx.outputs().iter().enumerate() {
        if output.value == 0 {
            return Err(BlockValidationError::ZeroValueOutput {
                tx_index: index,
                output_index: j,
            });
        }
        sum = sum
            .checked_add(output.value)
            .ok_or(BlockValidationError::ValueOverflow { tx_index: index })?;
    }

    Ok(())
}

/// A [`BlockValidator`] that enforces [`validate_block_payload`] on every block
/// inserted into a [`Dag`]. It is stateless — install one instance and share it.
#[derive(Clone, Copy, Debug, Default)]
pub struct TxStructureValidator;

impl BlockValidator for TxStructureValidator {
    fn validate(&self, block: &Block, _dag: &Dag) -> Result<(), String> {
        validate_block_payload(block.payload()).map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keys::KeyPair;
    use crate::tx::{encode_block_payload, TxId, TxOutput};

    fn addr(seed: u64) -> crate::keys::Address {
        KeyPair::from_u64(seed).address()
    }

    fn coin(seed: u8) -> OutPoint {
        OutPoint::new(TxId::from_bytes([seed; 32]), 0)
    }

    #[test]
    fn well_formed_block_passes() {
        let kp = KeyPair::from_u64(1);
        let cb = Transaction::coinbase(vec![TxOutput::new(50, addr(1))], b"h1".to_vec());
        let transfer =
            Transaction::signed(&[(coin(7), &kp)], vec![TxOutput::new(20, addr(2))], vec![]);
        let payload = encode_block_payload(&[cb, transfer]);
        assert_eq!(validate_block_payload(&payload), Ok(()));
    }

    #[test]
    fn undecodable_payload_is_rejected() {
        let err = validate_block_payload(b"not-transactions").unwrap_err();
        assert!(matches!(err, BlockValidationError::Payload(_)));
    }

    #[test]
    fn coinbase_must_be_first() {
        let cb = Transaction::coinbase(vec![TxOutput::new(50, addr(1))], b"h1".to_vec());
        let kp = KeyPair::from_u64(1);
        let transfer =
            Transaction::signed(&[(coin(7), &kp)], vec![TxOutput::new(20, addr(2))], vec![]);
        // Coinbase in second position.
        let payload = encode_block_payload(&[transfer, cb]);
        assert_eq!(
            validate_block_payload(&payload),
            Err(BlockValidationError::MisplacedCoinbase { tx_index: 1 })
        );
    }

    #[test]
    fn non_coinbase_needs_outputs() {
        let kp = KeyPair::from_u64(1);
        let no_outputs = Transaction::signed(&[(coin(7), &kp)], vec![], vec![]);
        let payload = encode_block_payload(&[no_outputs]);
        assert_eq!(
            validate_block_payload(&payload),
            Err(BlockValidationError::NoOutputs { tx_index: 0 })
        );
    }

    #[test]
    fn duplicate_input_is_rejected() {
        let kp = KeyPair::from_u64(1);
        let op = coin(7);
        let dup = Transaction::signed(
            &[(op, &kp), (op, &kp)],
            vec![TxOutput::new(5, addr(2))],
            vec![],
        );
        let payload = encode_block_payload(&[dup]);
        assert_eq!(
            validate_block_payload(&payload),
            Err(BlockValidationError::DuplicateInput {
                tx_index: 0,
                outpoint: op
            })
        );
    }

    #[test]
    fn zero_value_output_is_rejected() {
        let kp = KeyPair::from_u64(1);
        let zero = Transaction::signed(&[(coin(7), &kp)], vec![TxOutput::new(0, addr(2))], vec![]);
        let payload = encode_block_payload(&[zero]);
        assert_eq!(
            validate_block_payload(&payload),
            Err(BlockValidationError::ZeroValueOutput {
                tx_index: 0,
                output_index: 0
            })
        );
    }

    #[test]
    fn overflowing_outputs_are_rejected() {
        let kp = KeyPair::from_u64(1);
        let overflow = Transaction::signed(
            &[(coin(7), &kp)],
            vec![TxOutput::new(u64::MAX, addr(2)), TxOutput::new(1, addr(3))],
            vec![],
        );
        let payload = encode_block_payload(&[overflow]);
        assert_eq!(
            validate_block_payload(&payload),
            Err(BlockValidationError::ValueOverflow { tx_index: 0 })
        );
    }

    #[test]
    fn tx_too_large_is_rejected() {
        let kp = KeyPair::from_u64(1);
        // Create a transaction with a huge tag to exceed MAX_TX_SIZE
        let huge_tag = vec![0u8; MAX_TX_SIZE + 100];
        let large_tx =
            Transaction::signed(&[(coin(7), &kp)], vec![TxOutput::new(5, addr(2))], huge_tag);
        let tx_size = large_tx.encode().len();
        let payload = encode_block_payload(&[large_tx]);
        assert_eq!(
            validate_block_payload(&payload),
            Err(BlockValidationError::TxTooLarge {
                tx_index: 0,
                size: tx_size
            })
        );
    }

    #[test]
    fn block_payload_too_large_is_rejected() {
        let kp = KeyPair::from_u64(1);
        let huge_tag = vec![0u8; MAX_BLOCK_PAYLOAD_SIZE + 100];
        let large_tx =
            Transaction::signed(&[(coin(7), &kp)], vec![TxOutput::new(5, addr(2))], huge_tag);
        let payload = encode_block_payload(&[large_tx]);
        assert_eq!(
            validate_block_payload(&payload),
            Err(BlockValidationError::BlockPayloadTooLarge {
                size: payload.len()
            })
        );
    }

    #[test]
    fn too_many_transactions_is_rejected() {
        let kp = KeyPair::from_u64(1);
        let txs: Vec<Transaction> = (0..MAX_TXS_PER_BLOCK + 10)
            .map(|i| {
                Transaction::signed(
                    &[(coin(i as u8), &kp)],
                    vec![TxOutput::new(5, addr(2))],
                    vec![i as u8],
                )
            })
            .collect();
        let payload = encode_block_payload(&txs);
        assert_eq!(
            validate_block_payload(&payload),
            Err(BlockValidationError::TooManyTransactions { count: txs.len() })
        );
    }
}
