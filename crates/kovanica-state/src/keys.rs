//! Ed25519 keys and addresses for authorising spends.
//!
//! An [`Address`] is the raw bytes of an ed25519 public key; a [`TxOutput`]
//! records the address that owns it (see [`crate::tx`]). To spend an output, a
//! transaction input must carry a signature that [`verify`]s against that
//! address over the transaction's signature hash.
//!
//! For humans, an address renders as `kvnc…dag` — base58 over the 32 key
//! bytes ([`Address::to_kvnc`]) — while the wire and ledger formats keep the
//! raw bytes (64 hex). [`Address::parse`] accepts either rendering.
//!
//! [`KeyPair`] is a thin, deterministic wrapper over an ed25519 signing key —
//! deterministic construction ([`KeyPair::from_seed`] / [`KeyPair::from_u64`])
//! keeps tests and tooling reproducible without a random source. Verification
//! uses `verify_strict`, which rejects non-canonical / malleable signatures so
//! that signature validity is a pure function of the bytes on every node.
//!
//! [`TxOutput`]: crate::tx::TxOutput

use core::fmt;

use ed25519_dalek::{Signature, Signer, SigningKey, VerifyingKey};

/// A 32-byte account address: the raw bytes of an ed25519 public key.
///
/// Ordering is over the raw bytes so any tie-break that falls back to an
/// address is deterministic across nodes.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Address([u8; 32]);

impl Address {
    /// Construct an address from raw public-key bytes.
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// The raw 32 public-key bytes.
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Lowercase hex rendering of the address.
    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }

    /// Human address: `kvnc` + base58(32-byte pubkey) + `dag`.
    /// Wire / ledger still uses the 32 raw bytes (64 hex).
    pub fn to_kvnc(&self) -> String {
        format!("kvnc{}dag", b58_encode(&self.0))
    }

    /// Parse 64-hex **or** `kvnc…dag` (case-insensitive wrapper).
    pub fn parse(s: &str) -> Result<Self, &'static str> {
        let t = s.trim();
        if t.len() == 64 && t.bytes().all(|b| b.is_ascii_hexdigit()) {
            let mut out = [0u8; 32];
            let raw = hex::decode(t).map_err(|_| "address is not hex")?;
            if raw.len() != 32 {
                return Err("address must be 32 bytes");
            }
            out.copy_from_slice(&raw);
            return Ok(Self(out));
        }
        if t.len() < 8
            || !t.get(..4).is_some_and(|p| p.eq_ignore_ascii_case("kvnc"))
            || !t
                .get(t.len() - 3..)
                .is_some_and(|p| p.eq_ignore_ascii_case("dag"))
        {
            return Err("address must be 64-hex or kvnc…dag");
        }
        let mid = &t[4..t.len() - 3];
        let bytes = b58_decode(mid)?;
        let arr: [u8; 32] = bytes
            .try_into()
            .map_err(|_| "kvnc address must be 32 bytes")?;
        Ok(Self(arr))
    }
}

impl fmt::Debug for Address {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Address({}…)", &self.to_hex()[..8])
    }
}

impl fmt::Display for Address {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

/// A signing keypair used to authorise spends and to derive an [`Address`].
///
/// Construct one deterministically from a seed; there is deliberately no
/// random constructor in this slice so that consensus and ledger tests stay
/// reproducible.
pub struct KeyPair {
    signing: SigningKey,
}

impl KeyPair {
    /// Build a keypair from a 32-byte seed (the ed25519 secret scalar seed).
    pub fn from_seed(seed: [u8; 32]) -> Self {
        Self {
            signing: SigningKey::from_bytes(&seed),
        }
    }

    /// Convenience: a deterministic keypair from a small integer seed. Handy in
    /// tests where distinct actors just need distinct, stable keys.
    pub fn from_u64(seed: u64) -> Self {
        let mut bytes = [0u8; 32];
        bytes[..8].copy_from_slice(&seed.to_le_bytes());
        Self::from_seed(bytes)
    }

    /// This keypair's address (its public-key bytes).
    pub fn address(&self) -> Address {
        Address(self.signing.verifying_key().to_bytes())
    }

    /// Sign `message`, returning the raw 64-byte signature. Ed25519 signatures
    /// are deterministic (RFC 8032), so this is a pure function of key + message.
    pub fn sign(&self, message: &[u8]) -> [u8; 64] {
        self.signing.sign(message).to_bytes()
    }
}

/// Verify that `signature` over `message` was produced by the holder of
/// `address`. Uses strict verification (rejects non-canonical / malleable
/// signatures and the identity point) so the result is deterministic.
///
/// Returns `false` if the address is not a valid public key or the signature
/// does not verify — a spend that fails here is simply unauthorised.
pub fn verify(address: &Address, message: &[u8], signature: &[u8; 64]) -> bool {
    let Ok(verifying_key) = VerifyingKey::from_bytes(address.as_bytes()) else {
        return false;
    };
    let signature = Signature::from_bytes(signature);
    verifying_key.verify_strict(message, &signature).is_ok()
}

const B58: &[u8] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

fn b58_encode(data: &[u8]) -> String {
    let zeros = data.iter().take_while(|b| **b == 0).count();
    let mut buf = data.to_vec();
    let mut digits = Vec::new();
    loop {
        if buf.iter().all(|b| *b == 0) {
            break;
        }
        let mut rem = 0u16;
        for b in buf.iter_mut() {
            let v = (rem << 8) | u16::from(*b);
            *b = (v / 58) as u8;
            rem = v % 58;
        }
        digits.push(B58[rem as usize]);
    }
    digits.reverse();
    let mut out = vec![b'1'; zeros];
    out.extend_from_slice(&digits);
    String::from_utf8(out).expect("base58 alphabet is ascii")
}

fn b58_decode(s: &str) -> Result<Vec<u8>, &'static str> {
    if s.is_empty() {
        return Err("empty kvnc payload");
    }
    // 40 accumulator bytes (LSB first) so overflow past 32 is detectable.
    let mut acc = [0u8; 40];
    for c in s.bytes() {
        let val = B58
            .iter()
            .position(|b| *b == c)
            .ok_or("invalid kvnc address character")?;
        let mut carry = val as u32;
        for b in acc.iter_mut().rev() {
            let v = u32::from(*b) * 58 + carry;
            *b = (v & 0xff) as u8;
            carry = v >> 8;
        }
        if carry != 0 {
            return Err("kvnc address overflow");
        }
    }
    // Reject anything wider than 32 bytes: without this check a payload
    // encoding 33–40 bytes would silently truncate onto a wrong address.
    if acc[..acc.len() - 32].iter().any(|b| *b != 0) {
        return Err("kvnc address too long");
    }
    Ok(acc[acc.len() - 32..].to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sign_verify_roundtrip() {
        let kp = KeyPair::from_u64(7);
        let msg = b"authorise this spend";
        let sig = kp.sign(msg);
        assert!(verify(&kp.address(), msg, &sig));
    }

    #[test]
    fn wrong_key_does_not_verify() {
        let signer = KeyPair::from_u64(1);
        let other = KeyPair::from_u64(2);
        let sig = signer.sign(b"m");
        assert!(!verify(&other.address(), b"m", &sig));
    }

    #[test]
    fn tampered_message_does_not_verify() {
        let kp = KeyPair::from_u64(3);
        let sig = kp.sign(b"pay alice");
        assert!(!verify(&kp.address(), b"pay mallory", &sig));
    }

    #[test]
    fn deterministic_address() {
        assert_eq!(
            KeyPair::from_u64(42).address(),
            KeyPair::from_u64(42).address()
        );
    }

    #[test]
    fn kvnc_display_roundtrip() {
        let addr = KeyPair::from_u64(1).address();
        let shown = addr.to_kvnc();
        assert!(shown.starts_with("kvnc"), "{shown}");
        assert!(shown.ends_with("dag"), "{shown}");
        assert!(shown.len() < 4 + 64 + 3, "should be shorter than hex wrap");
        assert_eq!(Address::parse(&shown).unwrap(), addr);
        assert_eq!(Address::parse(&format!("  {shown}  ")).unwrap(), addr);
        assert_eq!(Address::parse(&addr.to_hex()).unwrap(), addr);
        assert!(Address::parse("not-an-address").is_err());
        assert!(Address::parse("").is_err());
    }

    #[test]
    fn kvnc_roundtrips_many_addresses() {
        for seed in 0..64u64 {
            let addr = KeyPair::from_u64(seed).address();
            assert_eq!(
                Address::parse(&addr.to_kvnc()).unwrap(),
                addr,
                "seed {seed}"
            );
        }
    }

    #[test]
    fn b58_rejects_invalid_characters_and_overflow() {
        // '0', 'O', 'I', 'l' are not in the base58 alphabet.
        assert!(Address::parse("kvnc0OIl111dag").is_err());
        // 55 high digits exceed even the 40-byte accumulator.
        let long = "z".repeat(55);
        assert!(Address::parse(&format!("kvnc{long}dag")).is_err());
        // 50 high digits fit the accumulator but exceed 32 bytes of value.
        let wide = "z".repeat(50);
        assert!(Address::parse(&format!("kvnc{wide}dag")).is_err());
    }
}
