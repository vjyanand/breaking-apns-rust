use std::str::FromStr;
use std::time::Duration;

use http_body_util::Full;
use hyper::body::Bytes;
use hyper_tls::HttpsConnector;
use hyper_util::client::legacy::Client as LegacyClient;
use hyper_util::{client::legacy::connect::HttpConnector, rt::TokioExecutor};

use crate::config::ApnsConfig;

pub struct ApnsClient {
    client: LegacyClient<HttpsConnector<HttpConnector>, Full<Bytes>>,
    base_url: String,
    jwt_token: String,
}

impl ApnsClient {
    pub fn new(config: &ApnsConfig, jwt_token: &String) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let mut https_connector = HttpsConnector::new();
        https_connector.https_only(true);

        let client = LegacyClient::builder(TokioExecutor::new())
            .pool_idle_timeout(Duration::from_secs(5)) // Very short idle timeout
            .pool_max_idle_per_host(0) // No idle connections - close immediately
            .http2_only(true)
            .http2_initial_stream_window_size(32_768) // Minimal window size
            .http2_initial_connection_window_size(262_144) // 256KB - minimal
            .http2_adaptive_window(false) // Disable adaptive window
            .http2_keep_alive_interval(Duration::from_secs(5)) // Short keep-alive
            .http2_keep_alive_timeout(Duration::from_secs(3)) // Quick timeout
            .build(https_connector);

        let base_url = if config.sandbox { "https://api.sandbox.push.apple.com" } else { "https://api.push.apple.com" };
        let base_url = String::from_str(base_url)?;
        let jwt_token = String::from(jwt_token);
        Ok(Self { client, base_url, jwt_token })
    }
}
