mod apns;
mod client;
mod config;
mod processor;

use sqlx::{Pool, Postgres};

use crate::client::ApnsClient;
use crate::config::ApnsConfig;
use crate::processor::ApnsProcessor;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let config = ApnsConfig {
        key_id: "9F437T6Y4G".to_string(),
        team_id: "JX83D66C47".to_string(),
        private_key: std::fs::read_to_string("key.p8")?,
        sandbox: false,
    };

    let client = ApnsClient::new(config)?;
    let processor = ApnsProcessor::new(client, 10);
    // Connect to Postgres
    let pool =
        Pool::<Postgres>::connect("postgres://breaking:qwertY123@db.iavian.net/breaking").await?;

    let device_hash = Some("B3C1E811-AF76-4E98-BED0-5F7D63B034B9".to_owned());

    processor
        .process_notifications(pool, 402028, device_hash)
        .await;

    Ok(())
}
