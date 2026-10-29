use std::convert::TryInto;

use anyhow::{Context, Result, anyhow, bail};
use base64::Engine;
use base64::engine::general_purpose::STANDARD_NO_PAD;
use ed25519_dalek::{Signature, Verifier, VerifyingKey};

pub struct PackVerifier {
    key: VerifyingKey,
}

impl PackVerifier {
    pub fn from_env_value(value: &str) -> Result<Self> {
        let (alg, key_b64) = value
            .split_once(':')
            .ok_or_else(|| anyhow!("invalid PACK_PUBLIC_KEY format"))?;
        if !alg.eq_ignore_ascii_case("ed25519") {
            bail!("unsupported public key algorithm `{alg}`");
        }
        let raw = STANDARD_NO_PAD
            .decode(key_b64.trim())
            .or_else(|_| base64::engine::general_purpose::STANDARD.decode(key_b64.trim()))
            .context("PACK_PUBLIC_KEY is not valid base64")?;
        let key_bytes: &[u8; 32] = raw
            .as_slice()
            .try_into()
            .map_err(|_| anyhow!("PACK_PUBLIC_KEY must be 32 bytes"))?;
        let key = VerifyingKey::from_bytes(key_bytes)
            .map_err(|_| anyhow!("PACK_PUBLIC_KEY is not a valid Ed25519 key"))?;
        Ok(Self { key })
    }

    pub fn verify(&self, message: &[u8], signature_b64: &str) -> Result<()> {
        let sig_bytes = STANDARD_NO_PAD
            .decode(signature_b64.trim())
            .or_else(|_| base64::engine::general_purpose::STANDARD.decode(signature_b64.trim()))
            .context("pack signature is not valid base64")?;
        let signature = Signature::from_slice(&sig_bytes)
            .map_err(|_| anyhow!("pack signature is not a valid Ed25519 signature"))?;
        self.key
            .verify(message, &signature)
            .context("pack signature verification failed")
    }
}

/// Parse a (possibly multi-key) `PACK_PUBLIC_KEY` value into verifiers.
///
/// Keys are separated by commas, semicolons or whitespace, which allows
/// key rotation (old + new key configured at the same time).
pub fn parse_verifiers(value: &str) -> Result<Vec<PackVerifier>> {
    let verifiers = value
        .split(|ch: char| ch == ',' || ch == ';' || ch.is_whitespace())
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(PackVerifier::from_env_value)
        .collect::<Result<Vec<_>>>()?;
    Ok(verifiers)
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::engine::general_purpose::STANDARD;
    use ed25519_dalek::{Signer, SigningKey};

    #[test]
    fn accepts_valid_signatures() {
        let secret = SigningKey::from_bytes(&[7u8; 32]);
        let public_b64 = STANDARD.encode(secret.verifying_key().as_bytes());
        let verifier = PackVerifier::from_env_value(&format!("ed25519:{public_b64}")).unwrap();
        let message = b"sha256:deadbeef";
        let signature = secret.sign(message);
        let signature_b64 = STANDARD.encode(signature.to_bytes());
        verifier.verify(message, &signature_b64).unwrap();
    }

    #[test]
    fn parse_verifiers_accepts_multiple_keys_for_rotation() {
        let old = SigningKey::from_bytes(&[3u8; 32]);
        let new = SigningKey::from_bytes(&[4u8; 32]);
        let value = format!(
            "ed25519:{}, ed25519:{}",
            STANDARD.encode(old.verifying_key().as_bytes()),
            STANDARD.encode(new.verifying_key().as_bytes())
        );
        let verifiers = parse_verifiers(&value).unwrap();
        assert_eq!(verifiers.len(), 2);
        let sig = STANDARD.encode(new.sign(b"msg").to_bytes());
        assert!(verifiers[0].verify(b"msg", &sig).is_err());
        assert!(verifiers[1].verify(b"msg", &sig).is_ok());
    }

    #[test]
    fn rejects_invalid_signatures() {
        let secret = SigningKey::from_bytes(&[1u8; 32]);
        let public_b64 = STANDARD.encode(secret.verifying_key().as_bytes());
        let verifier = PackVerifier::from_env_value(&format!("ed25519:{public_b64}")).unwrap();
        let bad_sig = STANDARD.encode([0xAAu8; 64]);
        assert!(verifier.verify(b"msg", &bad_sig).is_err());
    }

    #[test]
    fn rejects_unsupported_public_key_algorithms() {
        let err = PackVerifier::from_env_value("rsa:Zm9v")
            .err()
            .expect("algorithm should be rejected");
        assert!(err.to_string().contains("unsupported public key algorithm"));
    }

    #[test]
    fn rejects_public_keys_with_wrong_length() {
        let err = PackVerifier::from_env_value("ed25519:Zm9v")
            .err()
            .expect("short public key should be rejected");
        assert!(err.to_string().contains("must be 32 bytes"));
    }

    #[test]
    fn rejects_malformed_signature_encoding() {
        let secret = SigningKey::from_bytes(&[9u8; 32]);
        let public_b64 = STANDARD.encode(secret.verifying_key().as_bytes());
        let verifier = PackVerifier::from_env_value(&format!("ed25519:{public_b64}")).unwrap();

        let err = verifier
            .verify(b"msg", "!not-base64!")
            .expect_err("malformed signature should be rejected");
        assert!(err.to_string().contains("not valid base64"));
    }
}
