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
    worker_count: usize,
    max_concurrent_requests: usize,
}

impl ApnsProcessor {
    pub fn new(client: ApnsClient, worker_count: usize) -> Self {
        Self {
            client: Arc::new(client),
            worker_count,
            max_concurrent_requests: worker_count * 50,
        }
    }

    pub async fn process_notifications(
        &self,
        pool: Pool<Postgres>,
        news_id: i64,
        device_hash: Option<&String>,
    ) {
        //let pool = Arc::new(pool);
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
        let (tx, rx) = mpsc::channel(51000);
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
        });

        let mut join_set = JoinSet::new();
        let rx = Arc::new(tokio::sync::Mutex::new(rx));
        let w_client = Arc::clone(&self.client);

        for _ in 0..self.worker_count {
            let worker_pool = Arc::clone(&pool);
            let worker_client = Arc::clone(&w_client);
            let worker_rx = Arc::clone(&rx);
            let worker_semaphore = Arc::clone(&semaphore);

            join_set.spawn(async move {
                let mut worker_join_set = JoinSet::new();
                loop {
                    let notif = {
                        let mut rx_guard = worker_rx.lock().await;
                        match rx_guard.recv().await {
                            Some(notif) => notif,
                            None => break, // Channel closed
                        }
                    };

                    let permit = worker_semaphore.clone().acquire_owned().await;
                    if let Ok(_permit) = permit {
                        let sub_worker_client = Arc::clone(&worker_client);
                        let sub_worker_pool = Arc::clone(&worker_pool);
                        worker_join_set.spawn(async move {
                            match sub_worker_client.send_notification(notif).await {
                                Ok(result) => {
                                    if !result.success && result.status_code == 410 {
                                        warn!(
                                            "DB-DELETE-APNS Failure: ID={}, APNS-ID={:?}, Status={}, Error={:?}",
                                            result.notification_id, result.apns_id, result.status_code, result.error
                                        );
                                        if let Err(err) = sqlx::query("UPDATE apns_master SET news_id = 0 WHERE id = $1").bind(result.notification_id).execute(&*sub_worker_pool).await {
                                             warn!("Failed to updated failed APNS notification: {err}");
                                        }
                                    } else if !result.success {
                                        warn!("Failed to send APNS notification: {result:?}");
                                    }
                                },
                                Err(err) => {
                                    warn!("Error sending notification: {err}");
                                }
                            }
                        });
                    };
                }
                while let Some(result) = worker_join_set.join_next().await {
                    if let Err(e) = result {
                        warn!("Individual request failed: {e}");
                    }
                }
            });
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
