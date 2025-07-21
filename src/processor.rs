use std::sync::Arc;

use log::warn;
use sqlx::{Pool, Postgres};
use tokio::{sync::mpsc, task::JoinSet};
use tokio_stream::StreamExt;

use crate::apns::PushNotification;
use crate::client::ApnsClient;

pub struct ApnsProcessor {
    client: ApnsClient,
    worker_count: usize,
}

impl ApnsProcessor {
    pub fn new(client: ApnsClient, worker_count: usize) -> Self {
        Self {
            client,
            worker_count,
        }
    }
    pub async fn process_notifications(
        &self,
        pool: Pool<Postgres>,
        news_id: i64,
        device_hash: Option<&String>,
    ) {
        let (tx, rx) = mpsc::channel(1000);

        let mut device_hash_filter = String::new();
        if let Some(device_hash) = device_hash {
            device_hash_filter = format!("dm.devicehash = '{device_hash}' AND");
        }
        let sql = format!(
            "SELECT nm.url, dm.id, nm.id AS nId, dm.devicehash, dm.token, dm.sound_id, trim(nm.text) AS text, (case when _from AT TIME ZONE 'UTC' < _to AT TIME ZONE 'UTC' then ((_from, _to) OVERLAPS (current_time, current_time)) else (case when _from <= current_time OR _to >= current_time then true else false end) end) AS playsound, dm.paid, extract(epoch from nm.news_date)::BIGINT AS news_date, dm.type, nm.news_id FROM apns_master dm, news_master nm where (dm.news_id & nm.news_id = nm.news_id) AND {device_hash_filter} nm.id = $1"
        );
        let pool = Arc::new(pool);
        let fetch_pool = Arc::clone(&pool);
        let fetch_task = tokio::spawn(async move {
            let mut stream = sqlx::query_as::<_, PushNotification>(&sql)
                .bind(news_id)
                .fetch(&*fetch_pool);
            while let Some(notification) = stream.next().await {
                match notification {
                    Ok(notification) => {
                        if tx.send(notification).await.is_err() {
                            break;
                        }
                    }
                    Err(err) => {
                        warn!("Error fetching notification: {err}");
                        continue;
                    }
                }
            }
            drop(tx);
        });

        let shared_rx = Arc::new(tokio::sync::Mutex::new(rx));
        let mut join_set = JoinSet::new();
        for _ in 0..self.worker_count {
            let worker_rx = Arc::clone(&shared_rx);
            let client = self.client.clone();
            let worker_pool = Arc::clone(&pool);
            join_set.spawn(async move {
                loop {
                    let notification = {
                        let mut rx_guard = worker_rx.lock().await;
                        rx_guard.recv().await
                    };
                    match notification {
                        Some(notif) => match client.send_notification(notif).await {
                            Ok(result) => {
                                if !result.success && result.status_code == 410 {
                                    if let Err(err) =
                                        sqlx::query("DELETE FROM apns_master WHERE id = $1")
                                            .bind(result.notification_id)
                                            .execute(&*worker_pool)
                                            .await
                                    {
                                        warn!("Failed to delete APNs notification: {err}");
                                    }
                                } else if !result.success {
                                    warn!("Failed to send APNs notification: {result:?}");
                                }
                            }
                            Err(err) => {
                                warn!("Error sending notification: {err}");
                            }
                        },
                        None => break, // Channel closed
                    }
                }
            });
        }

        let _ = fetch_task.await;
        while let Some(result) = join_set.join_next().await {
            if let Err(e) = result {
                warn!("Worker task failed: {e}");
            }
        }
    }
}
