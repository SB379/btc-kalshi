use anyhow::Context;
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use rsa::{
    pkcs1v15::SigningKey,
    signature::{SignatureEncoding, Signer},
    RsaPrivateKey,
};
use sha2::Sha256;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct KalshiAuth {
    pub key_id: String,
    pub private_key: RsaPrivateKey,
}

impl KalshiAuth {
    /// Read KALSHI_API_KEY_ID and KALSHI_PRIVATE_KEY (PEM string) from environment.
    /// Supports both PKCS#8 and PKCS#1 PEM formats.
    pub fn from_env() -> Result<Self, anyhow::Error> {
        let key_id =
            std::env::var("KALSHI_API_KEY_ID").context("KALSHI_API_KEY_ID env var not set")?;
        let pem =
            std::env::var("KALSHI_PRIVATE_KEY").context("KALSHI_PRIVATE_KEY env var not set")?;
        let private_key = parse_private_key(&pem)?;
        Ok(KalshiAuth { key_id, private_key })
    }

    /// Build the three Kalshi authentication headers for a request.
    ///
    /// message = timestamp_ms_string + METHOD_UPPERCASE + path
    /// signature = base64(PKCS1v15-SHA256-sign(message))
    pub fn sign_request(&self, method: &str, path: &str) -> Result<HeaderMap, anyhow::Error> {
        let ts_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let timestamp = ts_ms.to_string();

        let message = format!("{}{}{}", timestamp, method.to_uppercase(), path);
        let signing_key = SigningKey::<Sha256>::new(self.private_key.clone());
        let sig = signing_key.sign(message.as_bytes());
        let sig_b64 = BASE64.encode(sig.to_bytes().as_ref());

        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("kalshi-access-key"),
            HeaderValue::from_str(&self.key_id).context("key_id contains invalid header chars")?,
        );
        headers.insert(
            HeaderName::from_static("kalshi-access-timestamp"),
            HeaderValue::from_str(&timestamp).context("timestamp is invalid header value")?,
        );
        headers.insert(
            HeaderName::from_static("kalshi-access-signature"),
            HeaderValue::from_str(&sig_b64).context("signature is invalid header value")?,
        );
        Ok(headers)
    }
}

fn parse_private_key(pem: &str) -> Result<RsaPrivateKey, anyhow::Error> {
    // Try PKCS#8 first (most tools default to this format)
    use rsa::pkcs8::DecodePrivateKey;
    if let Ok(key) = RsaPrivateKey::from_pkcs8_pem(pem) {
        return Ok(key);
    }
    // Fall back to PKCS#1 (legacy/OpenSSL RSA format)
    use rsa::pkcs1::DecodeRsaPrivateKey;
    RsaPrivateKey::from_pkcs1_pem(pem)
        .context("failed to parse KALSHI_PRIVATE_KEY (tried PKCS#8 and PKCS#1 PEM formats)")
}

#[cfg(test)]
mod tests {
    use super::KalshiAuth;
    use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
    use rand::rngs::OsRng;
    use rsa::RsaPrivateKey;

    #[test]
    fn sign_request_produces_valid_headers() {
        let private_key =
            RsaPrivateKey::new(&mut OsRng, 2048).expect("failed to generate test RSA key");

        let auth = KalshiAuth { key_id: "test-key-id".to_string(), private_key };

        let headers = auth
            .sign_request("POST", "/portfolio/orders")
            .expect("sign_request should not fail");

        assert!(headers.contains_key("kalshi-access-key"), "missing access-key header");
        assert!(
            headers.contains_key("kalshi-access-timestamp"),
            "missing timestamp header"
        );
        assert!(
            headers.contains_key("kalshi-access-signature"),
            "missing signature header"
        );

        // Verify signature is valid non-empty base64
        let sig_str = headers
            .get("kalshi-access-signature")
            .expect("signature header present")
            .to_str()
            .expect("signature is valid UTF-8");
        let decoded = BASE64.decode(sig_str).expect("signature should be valid base64");
        assert!(!decoded.is_empty(), "decoded signature should not be empty");

        // Key header should match what we set
        let key_str = headers
            .get("kalshi-access-key")
            .expect("key header present")
            .to_str()
            .expect("key is valid UTF-8");
        assert_eq!(key_str, "test-key-id");
    }

    #[test]
    fn sign_request_timestamp_is_numeric() {
        let private_key =
            RsaPrivateKey::new(&mut OsRng, 2048).expect("failed to generate test RSA key");
        let auth = KalshiAuth { key_id: "k".to_string(), private_key };
        let headers = auth.sign_request("GET", "/portfolio/balance").expect("sign ok");
        let ts_str = headers
            .get("kalshi-access-timestamp")
            .expect("ts header present")
            .to_str()
            .expect("ts is valid UTF-8");
        let ts: u64 = ts_str.parse().expect("timestamp should be a valid u64");
        assert!(ts > 0, "timestamp should be positive");
    }
}
