use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::{Method, Request, Uri};
use hyper_tls::HttpsConnector;
use hyper_util::client::legacy::{Client as LegacyClient, connect::HttpConnector};
use hyper_util::rt::TokioExecutor;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sqlx::Row;
use sqlx::{FromRow, PgPool, Pool, Postgres};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{Mutex, broadcast, mpsc};
use tokio::task::JoinSet;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::UnboundedReceiverStream;
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
    #[serde(rename = "mutable-content", skip_serializing_if = "Option::is_none")]
    pub mutable_content: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub category: Option<String>,
}

impl Default for ApsPayload {
    fn default() -> Self {
        Self {
            alert: None,
            badge: None,
            sound: None,
            content_available: None,
            mutable_content: None,
            category: None,
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

    pub fn with_mutable_content(mut self) -> Self {
        self.mutable_content = Some(1);
        self
    }

    pub fn with_category<S: Into<String>>(mut self, category: S) -> Self {
        self.category = Some(category.into());
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

    pub fn mutable_content(mut self) -> Self {
        self.aps = self.aps.with_mutable_content();
        self
    }

    pub fn category<S: Into<String>>(mut self, category: S) -> Self {
        self.aps = self.aps.with_category(category);
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

pub struct PushNotification {
    pub id: i64,
    pub device_token: String,
    pub priority: Option<i32>,
    pub expiration: Option<u64>,
    pub collapse_id: Option<String>,
    pub push_type: Option<String>,
    pub title: Option<String>,
    pub payload: ApnsPayload,
}

impl PushNotification {
    pub fn new(id: i64, device_token: String, payload: ApnsPayload) -> Self {
        Self {
            id,
            device_token,
            priority: None,
            expiration: None,
            collapse_id: None,
            push_type: None,
            title: None,
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

    pub fn with_push_type<S: Into<String>>(mut self, push_type: S) -> Self {
        self.push_type = Some(push_type.into());
        self
    }
}

impl FromRow<'_, sqlx::postgres::PgRow> for PushNotification {
    fn from_row(row: &sqlx::postgres::PgRow) -> Result<Self, sqlx::Error> {
        let alert = AlertPayload {
            title: Some("".to_string()),
            subtitle: None,
            body: row.try_get("body")?,
        };
        let aps: ApsPayload = ApsPayload {
            alert: Some(alert),
            badge: None,
            sound: None,
            content_available: None,
            mutable_content: None,
            category: None,
        };
        let payload: ApnsPayload = ApnsPayload {
            aps,
            custom: Map::new(),
        };
        Ok(Self {
            id: row.try_get("id")?,
            device_token: row.try_get("device_token")?,
            priority: row.try_get("priority").ok(),
            expiration: Some(1),
            collapse_id: row.try_get("collapse_id").ok(),
            push_type: row.try_get("push_type").ok(),
            title: row.try_get("title").ok(),
            payload,
        })
    }
}

pub struct PushResult {
    pub notification_id: i64,
    pub device_token: String,
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

        if let Some(push_type) = &notification.push_type {
            request = request.header("apns-push-type", push_type);
        }

        let apns_id = Uuid::new_v4().to_string();
        request = request.header("apns-id", &apns_id);

        let request = request.body(Full::new(Bytes::from(payload)))?;

        let response = self.client.request(request).await?;
        let status = response.status();
        let headers = response.headers().clone();

        let apns_id_header = headers
            .get("apns-id")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string())
            .unwrap_or(apns_id);

        let body_bytes = response.collect().await?.to_bytes();
        let error_message = if !status.is_success() && !body_bytes.is_empty() {
            String::from_utf8_lossy(&body_bytes).to_string()
        } else {
            String::new()
        };

        let result = PushResult {
            notification_id: notification.id,
            device_token: notification.device_token,
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
                "APNs Failure: ID={}, Token={}, Status={}, Error={:?}",
                result.notification_id, result.device_token, result.status_code, result.error
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
        let mut device_hash_filter = String::new();
        if let Some(device_hash) = device_hash {
            device_hash_filter = format!("dm.devicehash = '{}' and", device_hash);
        }
        let sql = format!(
            "SELECT nm.url, dm.id, nm.id AS nId, dm.devicehash, dm.token, dm.sound_id, trim(nm.text) AS text, (case when _from AT TIME ZONE 'UTC' < _to AT TIME ZONE 'UTC' then ((_from, _to) OVERLAPS (current_time, current_time)) else (case when _from <= current_time OR _to >= current_time then true else false end) end) AS playsound, dm.paid, extract(epoch from nm.news_date) AS news_date, dm.type, nm.news_id FROM apns_master dm, news_master nm where %s (dm.news_id & nm.news_id = nm.news_id) %s AND {} nm.id = $1",
            device_hash_filter
        );

        println!("SQL Query: {}", sql);

        let mut stream = sqlx::query_as::<_, PushNotification>(&sql)
            .bind(news_id)
            .fetch(&pool);
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

    // Example 1: Using PayloadBuilder (Recommended)
    let payload = PayloadBuilder::new()
        .alert(Some("Breaking News"), "A new article has been published.")
        .badge(1)
        .sound("default")
        .custom("category", "news")
        .custom("priority", "high")
        .build();

    let notification = PushNotification::new(
        1,
        "4ffc5df2e74ea8e308caffffd22248aa2db666cd2ce64d474b118a935d105ce8".to_string(),
        payload,
    )
    .with_priority(10)
    .with_push_type("alert")
    .with_collapse_id(format!("collapse_{}", 1));

    let result = client.send_notification(notification).await?;

    if result.success {
        println!(
            "Notification sent successfully! APNs ID: {:?}",
            result.apns_id
        );
    } else {
        println!("Failed to send notification: {:?}", result.error);
    }

    Ok(())
}
