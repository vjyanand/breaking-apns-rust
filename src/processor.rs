use crate::client::ApnsClient;
use crate::{apns::PushNotification, config::ApnsConfig};
use futures_util::StreamExt;
use log::warn;
use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, PgConnection, Pool, Postgres};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::time::Duration;

pub struct ApnsProcessor {
    clients: Vec<ApnsClient>,
}

impl ApnsProcessor {
    pub async fn new(config: &ApnsConfig, num_clients: usize) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let mut clients = Vec::with_capacity(num_clients);
        let jwt_token = config.generate_jwt()?;
        for _ in 0..num_clients {
            if let Ok(client) = ApnsClient::new(config, &jwt_token) {
                clients.push(client);
            }
        }
        Ok(Self { clients })
    }
}
