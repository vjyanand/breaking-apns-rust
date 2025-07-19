use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::{Method, Request, Uri};
use hyper_tls::HttpsConnector;
use hyper_util::client::legacy::{Client as LegacyClient, connect::HttpConnector};
use hyper_util::rt::TokioExecutor;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Pool, Postgres};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinSet;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::UnboundedReceiverStream;
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
pub struct ApnsPayload {
    pub aps: ApsPayload,
    #[serde(flatten)]
    pub custom: HashMap<String, serde_json::Value>,
}

impl ApnsPayload {
    pub fn custom<T: Into<serde_json::Value>>(mut self, key: &str, value: T) -> Self {
        self.custom.insert(key.to_string(), value.into());
        self
    }
}

#[derive(Debug, Serialize, Deserialize)]
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
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AlertPayload {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    pub body: String,
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
    pub private_key: String, // P8 file content
    pub sandbox: bool,
}

impl ApnsConfig {
    pub fn generate_jwt(&self) -> Result<String, Box<dyn std::error::Error>> {
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
    jwt_cache: String,
}

impl ApnsClient {
    pub fn new(config: ApnsConfig) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let https_connector = HttpsConnector::new();
        let client = LegacyClient::builder(TokioExecutor::new())
            .pool_idle_timeout(std::time::Duration::from_secs(30))
            .pool_max_idle_per_host(100)
            .http2_only(true)
            .build(https_connector);

        let base_url = if config.sandbox {
            "https://api.sandbox.push.apple.com".to_string()
        } else {
            "https://api.push.apple.com".to_string()
        };
        let jwt_token = config.generate_jwt().unwrap();

        Ok(Self {
            client,
            config,
            base_url,
            jwt_cache: jwt_token,
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
            .header("authorization", format!("bearer {}", &self.jwt_cache))
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

        // Only log failures
        if !result.success {
            eprintln!(
                "APNs Failure: ID={}, Token={}, Status={}, Error={:?}",
                result.notification_id, result.device_token, result.status_code, result.error
            );
        }

        Ok(result)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // let database_url = std::env::var("DATABASE_URL")
    //    .unwrap_or_else(|_| "postgres://breaking:qwertY123@db.iavian.net/breaking".to_string());
    // let pool = PgPool::connect(&database_url).await?;

    let config = ApnsConfig {
        key_id: "9F437T6Y4G".to_string(),
        team_id: "JX83D66C47".to_string(),
        topic: "com.iavian.breakingnews".to_string(),
        private_key: std::fs::read_to_string("key.p8")?,
        sandbox: false,
    };
    let client = ApnsClient::new(config)?;

    let notification = PushNotification {
        id: 1,
        device_token: "4ffc5df2e74ea8e308caffffd22248aa2db666cd2ce64d474b118a935d105ce8"
            .to_string(),
        payload: ApnsPayload {
            aps: ApsPayload {
                alert: Some(AlertPayload {
                    title: Some("Breaking Newss".to_string()),
                    body: "A new article has been published.".to_string(),
                }),
                badge: Some(1),
                sound: Some("default".to_string()),
                content_available: None,
                mutable_content: None,
            },
            custom: HashMap::new(),
        }
        .custom("_u", "https://iavian.com")
        .custom("s", "CNN")
        .custom("nid", "1"),
        priority: Some(10),
        push_type: Some("alert".to_string()),
        expiration: None,
        title: Some("Test Notification".to_string()),
        collapse_id: format!("collapse_{}", 1).into(),
    };

    client.send_notification(notification).await?;
    Ok(())
}
