use crate::client::ApnsClient;
use crate::{apns::PushNotification, config::ApnsConfig};
use futures_util::StreamExt;
use log::warn;
use sqlx::{Connection, PgConnection};
use std::hash::{DefaultHasher, Hash, Hasher};

pub struct ApnsProcessor {
    clients: Vec<ApnsClient>,
}

impl ApnsProcessor {
    pub fn new(config: &ApnsConfig, num_clients: usize) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let mut clients = Vec::with_capacity(num_clients);
        let jwt_token = config.generate_jwt()?;
        for _ in 0..num_clients {
            if let Ok(client) = ApnsClient::new(config, &jwt_token) {
                clients.push(client);
            }
        }
        Ok(Self { clients })
    }

    pub async fn process_notifications(&self, news_id: i64, device_hash: Option<&String>) {
        let sql = match device_hash {
            Some(device_hash) => format!(
                "SELECT CASE WHEN LENGTH(COALESCE(nm.url, '')) < 3 THEN CONCAT('https://breaking.iavian.net/article/', nm.id)::TEXT ELSE url END AS url, dm.id, nm.id AS nId, dm.devicehash, dm.token, dm.sound_id, trim(nm.text) AS text, (case WHEN _from AT TIME ZONE 'UTC' < _to AT TIME ZONE 'UTC' THEN ((_from, _to) OVERLAPS (current_time, current_time)) ELSE (case WHEN _from <= current_time OR _to >= current_time THEN true ELSE false END) END) AS playsound, dm.paid, extract(epoch from nm.news_date)::BIGINT AS news_date, dm.type, nm.news_id FROM apns_master dm, news_master nm WHERE (dm.news_id & nm.news_id = nm.news_id) AND dm.devicehash = '{device_hash}' AND nm.id = $1"
            ),
            None => String::from(
                "SELECT CASE WHEN LENGTH(COALESCE(nm.url, '')) < 3 THEN CONCAT('https://breaking.iavian.net/article/', nm.id)::TEXT ELSE url END AS url, dm.id, nm.id AS nId, dm.devicehash, dm.token, dm.sound_id, trim(nm.text) AS text, (case WHEN _from AT TIME ZONE 'UTC' < _to AT TIME ZONE 'UTC' THEN ((_from, _to) OVERLAPS (current_time, current_time)) ELSE (case WHEN _from <= current_time OR _to >= current_time THEN true ELSE false END) END) AS playsound, dm.paid, extract(epoch from nm.news_date)::BIGINT AS news_date, dm.type, nm.news_id FROM apns_master dm, news_master nm WHERE (dm.news_id & nm.news_id = nm.news_id) AND nm.id = $1",
            ),
        };
        let Ok(mut conn) = PgConnection::connect("postgres://breaking:qwertY123@db.iavian.net/breaking").await else {
            warn!("Failed to get db connection");
            return;
        };
        let stream = sqlx::query_as::<_, PushNotification>(&sql).bind(news_id).fetch(&mut conn);
        stream
            .for_each_concurrent(Some(6000), |notification| async move {
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

                            if let Ok(mut inner_conn) = PgConnection::connect("postgres://breaking:qwertY123@db.iavian.net/breaking").await {
                                if let Err(err) = sqlx::query("UPDATE apns_master SET news_id = 0 WHERE id = $1").bind(apns_id).execute(&mut inner_conn).await {
                                    warn!("Failed to update APNS notification: {err}");
                                } else {
                                    warn!("DB-UPDATE-APNS-FAIL: ID={}, Status={}, Error={:?}", apns_id, result.status_code, result.error);
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
