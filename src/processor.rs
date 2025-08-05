use crate::apns::PushNotification;
use crate::client::ApnsClient;
use futures_util::StreamExt;
use log::warn;
use sqlx::{Pool, Postgres};
use std::sync::Arc;

pub struct ApnsProcessor {
    client: Arc<ApnsClient>,
    max_concurrent_requests: usize,
}

impl ApnsProcessor {
    pub fn new(client: ApnsClient) -> Self {
        Self { client: Arc::new(client), max_concurrent_requests: 1000 }
    }

    pub async fn process_notifications(&self, pool: Pool<Postgres>, news_id: i64, device_hash: Option<&String>) {
        let mut device_hash_filter = String::new();
        if let Some(device_hash) = device_hash {
            device_hash_filter = format!("dm.devicehash = '{device_hash}' AND");
        }
        let sql = format!(
            "SELECT nm.url, dm.id, nm.id AS nId, dm.devicehash, dm.token, dm.sound_id, trim(nm.text) AS text, \
            (case when _from AT TIME ZONE 'UTC' < _to AT TIME ZONE 'UTC' then ((_from, _to) OVERLAPS (current_time, current_time)) \
            else (case when _from <= current_time OR _to >= current_time then true else false end) end) AS playsound, \
            dm.paid, extract(epoch from nm.news_date)::BIGINT AS news_date, dm.type, nm.news_id \
            FROM apns_master dm, news_master nm \
            WHERE (dm.news_id & nm.news_id = nm.news_id) AND {device_hash_filter} nm.id = $1"
        );
       // let semaphore = Arc::new(Semaphore::new(self.max_concurrent_requests));
        let pool = Arc::new(pool);
        let stream = sqlx::query_as::<_, PushNotification>(&sql).bind(news_id).fetch(&*pool);
        stream
            .for_each_concurrent(self.max_concurrent_requests, |notification| {
               // let semaphore = Arc::clone(&semaphore);
                let client = Arc::clone(&self.client);
                let pool_ref = Arc::clone(&pool);
                async move {
                    if let Ok(notification) = notification {
                       // if let Ok(_permit) = semaphore.acquire().await {
                            match client.send_notification(notification).await {
                                Ok(push_result) => {
                                    if let Some(result) = push_result {
                                        if !result.success && result.status_code == 410 {
                                            if let Ok(apns_id) = result.apns_id {
                                                warn!("DB-UPDATE-APNS_FAIL: ID={}, Status={}, Error={:?}", apns_id, result.status_code, result.error);
                                                if let Err(err) = sqlx::query("UPDATE apns_master SET news_id = 0 WHERE id = $1").bind(apns_id).execute(&*pool_ref).await {
                                                    warn!("Failed to update APNS notification: {err}");
                                                }
                                            } else {
                                                warn!("Failed to parse APNS notification: {result:?}");
                                            }
                                        } else if !result.success {
                                            warn!("APNS notification send error: {result:?}");
                                        }
                                    }
                                }
                                Err(err) => {
                                    warn!("Error sending notification: {err}");
                                }
                            }
                       // }
                    }
                }
            })
            .await;
    }
}
