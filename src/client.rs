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
        let client = LegacyClient::builder(TokioExecutor::new()).http2_only(true).build(https_connector);
        let base_url = if config.sandbox { "https://api.sandbox.push.apple.com" } else { "https://api.push.apple.com" };
        let base_url = String::from_str(base_url)?;
        let jwt_token = String::from(jwt_token);
        Ok(Self { client, base_url, jwt_token })
    }
}
