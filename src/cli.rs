use log::warn;
use sqlx::{Pool, Postgres};

use crate::{client::ApnsClient, config::ApnsConfig, processor::ApnsProcessor};

mod apns;
mod client;
mod config;
mod handler;
mod processor;

#[tokio::main]
async fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("warn"))
        .format_timestamp_secs() // Include timestamps in seconds
        .init();

    let private_key = std::fs::read_to_string("key.p8").unwrap();

    let config = ApnsConfig { key_id: "9F437T6Y4G".to_string(), team_id: "JX83D66C47".to_string(), private_key, sandbox: false };

    let client = match ApnsClient::new(&config) {
        Ok(client) => client,
        Err(_e) => {
            return;
        }
    };
    warn!("Connecting to Postgres");
    let pool = match Pool::<Postgres>::connect("postgres://breaking:qwertY123@db.iavian.net/breaking").await {
        Ok(pool) => pool,
        Err(_e) => {
            return;
        }
    };
    warn!("Connected to Postgres");
    let processor = ApnsProcessor::new(client);
    let _device_hash = Some("AF2DA8D0-A553-4097-89DC-A140F5C039FB".to_owned());
    processor.process_notifications(pool, 403095, _device_hash.as_ref()).await;
    warn!("Finished processing notifications");
}
