use std::{str::FromStr, time::Duration};

use crate::{
    apns::{PushNotification, PushResult},
    config::ApnsConfig,
};
use http_body_util::{BodyExt, Full};
use hyper::{Method, Request, Uri, body::Bytes};
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use hyper_util::{
    client::legacy::{Client, connect::HttpConnector},
    rt::{TokioExecutor, TokioTimer},
};
use uuid::Uuid;

pub struct ApnsClient {
    client: Client<HttpsConnector<HttpConnector>, Full<Bytes>>,
    base_url: String,
    jwt_token: String,
}

impl ApnsClient {
    pub fn new(config: &ApnsConfig, jwt_token: &String) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let base_url = if config.sandbox { "https://api.sandbox.push.apple.com" } else { "https://api.push.apple.com" };
        let https = HttpsConnectorBuilder::new().with_native_roots()?.https_only().enable_http2().build();
        let client: Client<HttpsConnector<HttpConnector>, Full<Bytes>> = Client::builder(TokioExecutor::new())
            .pool_timer(TokioTimer::new())
            .pool_idle_timeout(Duration::from_secs(90))
            .pool_max_idle_per_host(10)
            .http2_only(true)
            .http2_adaptive_window(true)
            .http2_initial_stream_window_size(2 * 1024 * 1024) // 2MB
            .http2_initial_connection_window_size(4 * 1024 * 1024) // 4MB
            .http2_keep_alive_interval(Duration::from_secs(10))
            .http2_keep_alive_timeout(Duration::from_secs(20))
            .http2_keep_alive_while_idle(true)
            .build(https);

        let base_url = String::from_str(base_url)?;
        let jwt_token = String::from(jwt_token);
        Ok(Self { client, base_url, jwt_token })
    }

    pub async fn send_notification(&self, notification: &PushNotification) -> Result<Option<PushResult>, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{}/3/device/{}", self.base_url, notification.device_token);
        let uri: Uri = url.parse().unwrap();
        let payload = serde_json::to_string(&notification.payload).unwrap();
        let topic = match notification.push_type {
            crate::apns::BreakingApnsType::Complication => "com.iavian.breakingnews.watchkitapp.complication",
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

        if !status.is_success() {
            let apns_id_header = response
                .headers()
                .get("apns-id")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string())
                .unwrap_or(notification.id.to_string());
            let apns_id = Uuid::parse_str(&apns_id_header);
            let body_bytes = response.collect().await?.to_bytes();
            let error_message = String::from_utf8_lossy(&body_bytes).to_string();
            return Ok(Some(PushResult { apns_id, status_code: status.as_u16(), error: error_message }));
        }
        Ok(None)
    }
}
