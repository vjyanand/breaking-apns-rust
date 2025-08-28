use crate::client::ApnsClient;
use crate::{apns::PushNotification, config::ApnsConfig};
use futures_util::StreamExt;
use log::warn;
use sqlx::postgres::PgPoolOptions;
use sqlx::{Pool, Postgres};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::time::Duration;

pub struct ApnsProcessor {
    clients: Vec<ApnsClient>,
    pool: Pool<Postgres>,
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
        let pool = PgPoolOptions::new()
            .max_connections(3)
            .min_connections(1)
            .idle_timeout(Duration::from_secs(60))
            .max_lifetime(Duration::from_secs(400))
            .connect("postgres://breaking:qwertY123@db.iavian.net/breaking")
            .await?;

        Ok(Self { clients, pool })
    }

    pub async fn process_notifications(&self, news_id: i64, device_hash: Option<&String>) {
        let mut device_hash_filter = String::new();
        if let Some(device_hash) = device_hash {
            device_hash_filter = format!("dm.devicehash = '{device_hash}' AND");
        }
        let sql = format!(
            "SELECT CASE WHEN LENGTH(COALESCE(nm.url, '')) < 3 THEN CONCAT('https://breaking.iavian.net/article/', nm.id)::TEXT ELSE url END AS url, dm.id, nm.id AS nId, dm.devicehash, dm.token, dm.sound_id, trim(nm.text) AS text, \
            (case WHEN _from AT TIME ZONE 'UTC' < _to AT TIME ZONE 'UTC' THEN ((_from, _to) OVERLAPS (current_time, current_time)) \
            ELSE (case WHEN _from <= current_time OR _to >= current_time THEN true ELSE false END) END) AS playsound, \
            dm.paid, extract(epoch from nm.news_date)::BIGINT AS news_date, dm.type, nm.news_id \
            FROM apns_master dm, news_master nm \
            WHERE (dm.news_id & nm.news_id = nm.news_id) AND {device_hash_filter} nm.id = $1"
        );

        let stream = sqlx::query_as::<_, PushNotification>(&sql).bind(news_id).fetch(&self.pool);
        stream.for_each(|notification| async move { if let Ok(notification) = notification {} }).await;
    }
}
