use std::time::Duration;

use http_body_util::BodyExt;
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::{Method, Request, Uri};
use hyper_tls::HttpsConnector;
use hyper_util::client::legacy::Client as LegacyClient;
use hyper_util::{client::legacy::connect::HttpConnector, rt::TokioExecutor};

use crate::apns::PushNotification;
use crate::apns::PushResult;
use crate::config::ApnsConfig;

#[derive(Debug, Clone)]
pub struct ApnsClient {
    client: LegacyClient<HttpsConnector<HttpConnector>, Full<Bytes>>,
    base_url: String,
    jwt_token: String,
}

impl ApnsClient {
    pub fn new(config: &ApnsConfig) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let mut https_connector = HttpsConnector::new();
        https_connector.https_only(true);
        let client = LegacyClient::builder(TokioExecutor::new())
            .pool_idle_timeout(Duration::from_secs(60))
            .pool_max_idle_per_host(20)
            .http2_only(true)
            .http2_initial_stream_window_size(65_535) // Max stream window for HTTP/2
            .http2_initial_connection_window_size(1_048_576) // 1MB connection window
            .http2_adaptive_window(true)
            .build(https_connector);

        let base_url = if config.sandbox {
            "https://api.sandbox.push.apple.com".to_string()
        } else {
            "https://api.push.apple.com".to_string()
        };

        let jwt_token = config.generate_jwt()?;

        Ok(Self {
            client,
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
        let topic = match notification.push_type {
            crate::apns::BreakingApnsType::Complication => {
                "com.iavian.breakingnews.watchkitapp.complication"
            }
            crate::apns::BreakingApnsType::App => "com.iavian.breakingnews",
            crate::apns::BreakingApnsType::Watch => "com.iavian.breakingnews.watchkitapp",
        };

        let mut request = Request::builder()
            .method(Method::POST)
            .uri(uri)
            .header("authorization", format!("bearer {}", &self.jwt_token))
            .header("content-type", "application/json")
            .header("content-length", payload.len().to_string())
            .header("apns-topic", topic);

        if let Some(priority) = notification.priority {
            request = request.header("apns-priority", priority.to_string());
        }

        if let Some(expiration) = notification.expiration {
            request = request.header("apns-expiration", expiration.to_string());
        }

        if let Some(collapse_id) = &notification.collapse_id {
            request = request.header("apns-collapse-id", collapse_id.as_ref());
        }
        if notification.push_type == crate::apns::BreakingApnsType::Complication {
            request = request.header("apns-push-type", "complication");
        } else {
            request = request.header("apns-push-type", "alert");
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
        Ok(result)
    }
}
