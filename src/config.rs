use std::time::{SystemTime, UNIX_EPOCH};

use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use serde::Serialize;

#[derive(Debug, Clone)]
pub struct ApnsConfig {
    pub key_id: String,
    pub team_id: String,
    pub private_key: String,
    pub sandbox: bool,
}

impl ApnsConfig {
    pub fn generate_jwt(&self) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        #[derive(Serialize)]
        struct Claims {
            iss: String,
            iat: u64,
        }

        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();

        let claims = Claims {
            iss: self.team_id.clone(),
            iat: now,
        };

        let mut header = Header::new(Algorithm::ES256);
        header.kid = Some(self.key_id.clone());

        let key = EncodingKey::from_ec_pem(self.private_key.as_bytes())?;
        let token = encode(&header, &claims, &key)?;
        Ok(token)
    }
}
