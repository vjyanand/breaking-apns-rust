use crate::client::ApnsClient;
use crate::{apns::PushNotification, config::ApnsConfig};
use futures_util::StreamExt;
use log::warn;
use sqlx::{AssertSqlSafe, Connection, PgConnection};
use std::hash::{DefaultHasher, Hash, Hasher};

const POSTGRES_CONNECTION_STRING: &str = "postgres://breaking:qwertY123@db.iavian.net/breaking";
pub struct ApnsProcessor {
    clients: Vec<ApnsClient>,
    partition: u8,
}

impl ApnsProcessor {
    pub fn new(config: &ApnsConfig, partition: u8, num_clients: usize) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let mut clients = Vec::with_capacity(num_clients);
        let jwt_token = config.generate_jwt()?;
        for _ in 0..num_clients {
            if let Ok(client) = ApnsClient::new(config, &jwt_token) {
                clients.push(client);
            }
        }
        Ok(Self { clients, partition })
    }

    pub async fn process_notifications(&self, news_id: i64, device_hash: Option<&String>) {
        let sql = format!(
            "SELECT CASE WHEN LENGTH(COALESCE(nm.url, '')) < 3 THEN CONCAT('https://breaking.iavian.net/article/', nm.id)::TEXT ELSE url END AS url, dm.id, nm.id AS nId, dm.devicehash, dm.token, dm.sound_id, trim(nm.text) AS text, (case WHEN _from AT TIME ZONE 'UTC' < _to AT TIME ZONE 'UTC' THEN ((_from, _to) OVERLAPS (current_time, current_time)) ELSE (case WHEN _from <= current_time OR _to >= current_time THEN true ELSE false END) END) AS playsound, dm.paid, extract(epoch from nm.news_date)::BIGINT AS news_date, dm.type, nm.news_id FROM apns_master_p_{0} dm, news_master nm WHERE (dm.news_id & nm.news_id = nm.news_id) AND nm.id = $1{1}",
            self.partition,
            if device_hash.is_some() { " AND dm.devicehash = $2" } else { "" }
        );
        let Ok(mut conn) = PgConnection::connect(POSTGRES_CONNECTION_STRING).await else {
            warn!("Failed to get db connection");
            return;
        };
        let mut query = sqlx::query_as::<_, PushNotification>(AssertSqlSafe(sql)).bind(news_id);
        if let Some(device_hash) = device_hash {
            query = query.bind(device_hash.as_str());
        }
        let stream = query.fetch(&mut conn);
        stream
            .for_each_concurrent(Some(2000), |notification| async move {
                if let Ok(notification) = notification {
                    let mut hasher = DefaultHasher::new();
                    notification.device_token.hash(&mut hasher);
                    let hash = hasher.finish();
                    let index = (hash % self.clients.len() as u64) as usize;
                    let client = &self.clients[index];
                    match client.send_notification(&notification).await {
                        Ok(push_result) => {
                            let Some(result) = push_result else { return };
                            if result.status_code != 410 {
                                warn!("APNS notification send error: {result:?}");
                                return;
                            }
                            let Ok(apns_id) = result.apns_id else {
                                warn!("Failed to parse APNS notification: {result:?}");
                                return;
                            };

                            if let Ok(mut inner_conn) = PgConnection::connect(POSTGRES_CONNECTION_STRING).await {
                                if let Err(err) = sqlx::query("UPDATE apns_master_p SET news_id = 0 WHERE id = $1").bind(apns_id).execute(&mut inner_conn).await {
                                    warn!("DB-UPDATE-APNS-FAIL: ID={apns_id}, Status={}, Error={:?}, DbError={err}", result.status_code, result.error);
                                } else {
                                    warn!("DB-UPDATE-APNS-OK: ID={apns_id}, Status={}, Error={:?}", result.status_code, result.error);
                                }
                                let _ = inner_conn.close().await;
                            }
                        }
                        Err(err) => {
                            warn!("Error sending notification: {err}");
                        }
                    }
                }
            })
            .await;
        let _ = conn.close().await;
    }
}
