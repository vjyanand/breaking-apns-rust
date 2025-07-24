use crate::apns::PushNotification;
use crate::client::ApnsClient;
use log::warn;
use sqlx::{Pool, Postgres};
use std::sync::Arc;
use tokio::{sync::mpsc, task::JoinSet};
use tokio_stream::StreamExt;

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
        // Create a channel for distributing notifications to workers
        let (tx, mut rx) = mpsc::channel(10100);
        let pool = Arc::new(pool);

        // Construct the SQL query
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

        // Spawn a task to fetch notifications and send them to the channel
        let fetch_pool = Arc::clone(&pool);
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
                        continue;
                    }
                }
            }
            // Channel is dropped automatically when fetch_task completes
        });

        // Create worker channels
        let mut worker_txs = Vec::new();
        let mut join_set = JoinSet::new();
        for _ in 0..self.worker_count {
            let (worker_tx, mut worker_rx) = mpsc::channel(100);
            worker_txs.push(worker_tx);

            // Spawn a worker task
            let client = self.client.clone();
            let worker_pool = Arc::clone(&pool);
            join_set.spawn(async move {
                while let Some(notif) = worker_rx.recv().await {
                    match client.send_notification(notif).await {
                        Ok(result) => {
                            if !result.success && result.status_code == 410 {
                                warn!(
                                    "DB-DELETE-APNS Failure: ID={}, APNS-ID={:?}, Status={}, Error={:?}",
                                    result.notification_id, result.apns_id, result.status_code, result.error
                                );
                                if let Err(err) = sqlx::query("DELETE FROM apns_master WHERE id = $1")
                                    .bind(result.notification_id)
                                    .execute(&*worker_pool)
                                    .await
                                {
                                    warn!("Failed to delete APNS notification: {err}");
                                }
                            } else if !result.success {
                                warn!("Failed to send APNS notification: {result:?}");
                            }
                        }
                        Err(err) => {
                            warn!("Error sending notification: {err}");
                        }
                    }
                }
            });
        }

        // Distribute notifications to workers in a round-robin fashion
        let mut worker_index = 0;
        while let Some(notification) = rx.recv().await {
            if worker_txs[worker_index].send(notification).await.is_err() {
                warn!("Failed to send notification to worker {}", worker_index);
            }
            worker_index = (worker_index + 1) % self.worker_count;
        }

        // Drop all worker senders to close their channels
        drop(worker_txs);

        // Wait for fetch task and workers to complete
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
