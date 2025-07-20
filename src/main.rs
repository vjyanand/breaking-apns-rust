use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::{Method, Request, Uri};
use hyper_tls::HttpsConnector;
use hyper_util::client::legacy::{Client as LegacyClient, connect::HttpConnector};
use hyper_util::rt::TokioExecutor;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sqlx::{FromRow, Pool, Postgres};
use sqlx::{Row, Type};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_stream::StreamExt;
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ApnsPayload {
    pub aps: ApsPayload,
    #[serde(flatten)]
    pub custom: Map<String, Value>,
}

impl ApnsPayload {
    pub fn new(aps: ApsPayload) -> Self {
        Self {
            aps,
            custom: Map::new(),
        }
    }

    /// Add a custom field with any serializable value
    pub fn with_custom<T: Into<Value>>(mut self, key: impl Into<String>, value: T) -> Self {
        self.custom.insert(key.into(), value.into());
        self
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ApsPayload {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub alert: Option<AlertPayload>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub badge: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sound: Option<String>,
    #[serde(rename = "content-available", skip_serializing_if = "Option::is_none")]
    pub content_available: Option<i32>,
}

impl Default for ApsPayload {
    fn default() -> Self {
        Self {
            alert: None,
            badge: None,
            sound: None,
            content_available: None,
        }
    }
}

impl ApsPayload {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_alert(mut self, alert: AlertPayload) -> Self {
        self.alert = Some(alert);
        self
    }

    pub fn with_badge(mut self, badge: i32) -> Self {
        self.badge = Some(badge);
        self
    }

    pub fn with_sound<S: Into<String>>(mut self, sound: S) -> Self {
        self.sound = Some(sound.into());
        self
    }

    pub fn with_content_available(mut self) -> Self {
        self.content_available = Some(1);
        self
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AlertPayload {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subtitle: Option<String>,
    pub body: String,
}

impl AlertPayload {
    pub fn new<S: Into<String>>(body: S) -> Self {
        Self {
            title: None,
            subtitle: None,
            body: body.into(),
        }
    }

    pub fn with_title<S: Into<String>>(mut self, title: S) -> Self {
        self.title = Some(title.into());
        self
    }

    pub fn with_subtitle<S: Into<String>>(mut self, subtitle: S) -> Self {
        self.subtitle = Some(subtitle.into());
        self
    }
}

#[derive(Debug, Clone)]
pub struct PayloadBuilder {
    aps: ApsPayload,
    custom_fields: Map<String, Value>,
}

impl PayloadBuilder {
    pub fn new() -> Self {
        Self {
            aps: ApsPayload::new(),
            custom_fields: Map::new(),
        }
    }

    pub fn alert<S: Into<String>>(mut self, title: Option<S>, body: S) -> Self {
        let mut alert = AlertPayload::new(body);
        if let Some(t) = title {
            alert = alert.with_title(t);
        }
        self.aps = self.aps.with_alert(alert);
        self
    }

    pub fn badge(mut self, badge: i32) -> Self {
        self.aps = self.aps.with_badge(badge);
        self
    }

    pub fn sound<S: Into<String>>(mut self, sound: S) -> Self {
        self.aps = self.aps.with_sound(sound);
        self
    }

    pub fn content_available(mut self) -> Self {
        self.aps = self.aps.with_content_available();
        self
    }

    pub fn custom<T: Into<Value>>(mut self, key: impl Into<String>, value: T) -> Self {
        self.custom_fields.insert(key.into(), value.into());
        self
    }

    pub fn build(self) -> ApnsPayload {
        ApnsPayload {
            aps: self.aps,
            custom: self.custom_fields,
        }
    }
}

impl Default for PayloadBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub struct PushNotification {
    pub id: Uuid,
    pub device_token: String,
    pub priority: Option<i32>,
    pub expiration: Option<u64>,
    pub collapse_id: Option<String>,
    pub push_type: BreakingApnsType,
    pub payload: ApnsPayload,
}

impl PushNotification {
    pub fn new(id: Uuid, device_token: String, payload: ApnsPayload) -> Self {
        Self {
            id,
            device_token,
            priority: None,
            expiration: None,
            collapse_id: None,
            push_type: BreakingApnsType::App,
            payload,
        }
    }

    pub fn with_priority(mut self, priority: i32) -> Self {
        self.priority = Some(priority);
        self
    }

    pub fn with_expiration(mut self, expiration: u64) -> Self {
        self.expiration = Some(expiration);
        self
    }

    pub fn with_collapse_id<S: Into<String>>(mut self, collapse_id: S) -> Self {
        self.collapse_id = Some(collapse_id.into());
        self
    }
}

#[derive(Debug, Type, Serialize, Deserialize)]
#[sqlx(type_name = "breaking_apns_type", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum BreakingApnsType {
    App,
    Watch,
    Complication,
}
struct Sound(String);
impl From<i16> for Sound {
    fn from(sound_id: i16) -> Self {
        let sound = match sound_id {
            0 => "",
            1 => "default",
            2 => "g.caf",
            3 => "p.caf",
            4 => "s.caf",
            5 => "gl.caf",
            6 => "sm.caf",
            7 => "w.caf",
            _ => "default",
        };
        Sound(sound.to_string())
    }
}

impl FromRow<'_, sqlx::postgres::PgRow> for PushNotification {
    fn from_row(row: &sqlx::postgres::PgRow) -> Result<Self, sqlx::Error> {
        let paid: bool = row.try_get("paid")?;
        let playsound: bool = row.try_get("playsound")?;
        let sound_id: i16 = row.try_get("sound_id")?;
        let sound: Option<String> = if playsound {
            Some(Sound::from(sound_id).0)
        } else {
            None
        };
        let push_type: BreakingApnsType = row.try_get("type")?;
        let alert = AlertPayload {
            title: None,
            subtitle: Some("stitle".to_string()),
            body: row.try_get("text")?,
        };
        let aps: ApsPayload = ApsPayload {
            alert: Some(alert),
            badge: None,
            sound,
            content_available: None,
        };
        let payload: ApnsPayload = ApnsPayload {
            aps,
            custom: Map::new(),
        };
        Ok(Self {
            id: row.try_get("id")?,
            device_token: row.try_get("token")?,
            priority: Some(10),
            expiration: None,
            collapse_id: Some("1".to_owned()),
            push_type,
            payload,
        })
    }
}

#[derive(Debug)]
pub struct PushResult {
    pub notification_id: Uuid,
    pub success: bool,
    pub status_code: u16,
    pub apns_id: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ApnsConfig {
    pub key_id: String,
    pub team_id: String,
    pub topic: String,
    pub private_key: String,
    pub sandbox: bool,
}

impl ApnsConfig {
    pub fn generate_jwt(&self) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        #[derive(Serialize)]
        struct Claims {
            iss: String,
            iat: u64,
        }

        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();

        let claims = Claims {
            iss: self.team_id.clone(),
            iat: now,
        };

        let mut header = Header::new(Algorithm::ES256);
        header.kid = Some(self.key_id.clone());

        let key = EncodingKey::from_ec_pem(self.private_key.as_bytes())?;
        let token = encode(&header, &claims, &key)?;
        Ok(token)
    }
}

#[derive(Debug, Clone)]
pub struct ApnsClient {
    client: LegacyClient<HttpsConnector<HttpConnector>, Full<Bytes>>,
    config: ApnsConfig,
    base_url: String,
    jwt_token: String,
}

impl ApnsClient {
    pub fn new(config: ApnsConfig) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let https_connector = HttpsConnector::new();
        let client = LegacyClient::builder(TokioExecutor::new())
            .pool_idle_timeout(Duration::from_secs(30))
            .pool_max_idle_per_host(100)
            .http2_only(true)
            .build(https_connector);

        let base_url = if config.sandbox {
            "https://api.sandbox.push.apple.com".to_string()
        } else {
            "https://api.push.apple.com".to_string()
        };

        let jwt_token = config.generate_jwt()?;

        Ok(Self {
            client,
            config,
            base_url,
            jwt_token,
        })
    }

    pub async fn send_notification(
        &self,
        notification: PushNotification,
    ) -> Result<PushResult, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{}/3/device/{}", self.base_url, notification.device_token);
        let uri: Uri = url.parse()?;
        let payload = serde_json::to_string(&notification.payload)?;
        println!("APNs Payload: {}", payload);
        let mut request = Request::builder()
            .method(Method::POST)
            .uri(uri)
            .header("authorization", format!("bearer {}", &self.jwt_token))
            .header("content-type", "application/json")
            .header("content-length", payload.len().to_string())
            .header("apns-topic", &self.config.topic);

        if let Some(priority) = notification.priority {
            request = request.header("apns-priority", priority.to_string());
        }

        if let Some(expiration) = notification.expiration {
            request = request.header("apns-expiration", expiration.to_string());
        }

        if let Some(collapse_id) = &notification.collapse_id {
            request = request.header("apns-collapse-id", collapse_id);
        }

        request = request.header("apns-id", &notification.id.to_string());

        let request = request.body(Full::new(Bytes::from(payload)))?;

        let response = self.client.request(request).await?;
        let status = response.status();
        let headers = response.headers().clone();

        let apns_id_header = headers
            .get("apns-id")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string())
            .unwrap_or(notification.id.to_string());

        let body_bytes = response.collect().await?.to_bytes();
        let error_message = if !status.is_success() && !body_bytes.is_empty() {
            String::from_utf8_lossy(&body_bytes).to_string()
        } else {
            String::new()
        };

        let result = PushResult {
            notification_id: notification.id,
            success: status.is_success(),
            status_code: status.as_u16(),
            apns_id: Some(apns_id_header),
            error: if error_message.is_empty() {
                None
            } else {
                Some(error_message)
            },
        };

        if !result.success {
            eprintln!(
                "APNs Failure: ID={}, Status={}, Error={:?}",
                result.notification_id, result.status_code, result.error
            );
        }

        Ok(result)
    }
}

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
        device_hash: Option<String>,
    ) {
        let (tx, rx) = mpsc::channel(1000);

        let mut device_hash_filter = String::new();
        if let Some(device_hash) = device_hash {
            device_hash_filter = format!("dm.devicehash = '{}' AND", device_hash);
        }
        let sql = format!(
            "SELECT nm.url, dm.id, nm.id AS nId, dm.devicehash, dm.token, dm.sound_id, trim(nm.text) AS text, (case when _from AT TIME ZONE 'UTC' < _to AT TIME ZONE 'UTC' then ((_from, _to) OVERLAPS (current_time, current_time)) else (case when _from <= current_time OR _to >= current_time then true else false end) end) AS playsound, dm.paid, extract(epoch from nm.news_date) AS news_date, dm.type, nm.news_id FROM apns_master dm, news_master nm where (dm.news_id & nm.news_id = nm.news_id) AND {} nm.id = $1",
            device_hash_filter
        );

        // println!("SQL Query: {}", sql);
        let fetch_task = tokio::spawn(async move {
            let mut stream = sqlx::query_as::<_, PushNotification>(&sql)
                .bind(news_id)
                .fetch(&pool);
            while let Some(notification) = stream.next().await {
                match notification {
                    Ok(notification) => {
                        println!(
                            "Processing notification: ID={}, DeviceToken={}",
                            notification.id, notification.device_token
                        );
                        if tx.send(notification).await.is_err() {
                            break;
                        }
                    }
                    Err(err) => {
                        eprintln!("Error fetching notification: {}", err);
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
            join_set.spawn(async move {
                loop {
                    let notification = {
                        let mut rx_guard = worker_rx.lock().await;
                        rx_guard.recv().await
                    };
                    match notification {
                        Some(notif) => {
                            let result = client.send_notification(notif).await;
                            println!("{:?}", result);
                        }
                        None => break, // Channel closed
                    }
                }
            });
        }

        let _ = fetch_task.await;
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(_) => println!("Worker completed successfully"),
                Err(e) => eprintln!("Worker task panicked: {}", e),
            }
        }
    }
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let config = ApnsConfig {
        key_id: "9F437T6Y4G".to_string(),
        team_id: "JX83D66C47".to_string(),
        topic: "com.iavian.breakingnews".to_string(),
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
        .process_notifications(pool, 402001, device_hash)
        .await;

    Ok(())
}
