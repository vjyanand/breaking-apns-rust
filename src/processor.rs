use crate::apns::PushNotification;
use crate::client::ApnsClient;
use log::warn;
use sqlx::{Pool, Postgres};
use std::sync::Arc;
use tokio::{
    sync::{Semaphore, mpsc},
    task::JoinSet,
};
use tokio_stream::StreamExt;

pub struct ApnsProcessor {
    client: Arc<ApnsClient>,
    max_concurrent_requests: usize,
}

impl ApnsProcessor {
    pub fn new(client: ApnsClient) -> Self {
        Self {
            client: Arc::new(client),
            max_concurrent_requests: 10000,
        }
    }

    pub async fn process_notifications(
        &self,
        pool: Pool<Postgres>,
        news_id: i64,
        device_hash: Option<&String>,
    ) {
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
        let pool = Arc::new(pool);
        let fetch_pool = Arc::clone(&pool);
        let (tx, mut rx) = mpsc::channel(9000);
        let semaphore = Arc::new(Semaphore::new(self.max_concurrent_requests));
        let fetch_task = tokio::spawn(async move {
            let mut stream = sqlx::query_as::<_, PushNotification>(&sql)
                .bind(news_id)
                .fetch(&*fetch_pool);
            while let Some(notification) = stream.next().await {
                match notification {
                    Ok(notification) => {
                        if tx.send(notification).await.is_err() {
                            warn!("Channel closed prematurely");
                            break;
                        }
                    }
                    Err(err) => {
                        warn!("Error fetching notification: {err}");
                    }
                }
            }
            drop(tx);
        });

        let mut join_set = JoinSet::new();
        while let Some(notification) = rx.recv().await {
            let permit = semaphore.clone().acquire_owned().await;
            if let Ok(_permit) = permit {
                let client = Arc::clone(&self.client);
                let pool_ref = Arc::clone(&pool);
                join_set.spawn(async move {
                    let _permit = _permit;
                    match client.send_notification(notification).await {
                        Ok(result) => {
                            if let Some(result) = result {
                                if !result.success && result.status_code == 410 {
                                    if let Ok(apns_id) = result.apns_id {
                                        warn!(
                                            "DB-UPDATE-APNS_FAIL: ID={}, Status={}, Error={:?}",
                                            apns_id, result.status_code, result.error
                                        );
                                        if let Err(err) = sqlx::query(
                                            "UPDATE apns_master SET news_id = 0 WHERE id = $1",
                                        )
                                        .bind(apns_id)
                                        .execute(&*pool_ref)
                                        .await
                                        {
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
                });
            }
        }

        if let Err(e) = fetch_task.await {
            warn!("Fetch task failed: {e}");
        }

        while let Some(result) = join_set.join_next().await {
            if let Err(e) = result {
                warn!("Worker task failed: {e}");
            }
        }
    }
}
