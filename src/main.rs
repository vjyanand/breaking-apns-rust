mod apns;
mod client;
mod config;
mod handler;
mod processor;
use std::env;

use actix_web::middleware::{self, Logger};
use actix_web::web::Data;
use actix_web::{App, HttpServer};
use log::debug;

use crate::config::ApnsConfig;
use crate::handler::ok;
use crate::handler::push;
#[tokio::main]
async fn main() -> std::io::Result<()> {
    env_logger::init();
    debug!("Debug mode enabled");

    let port: u16 = env::var("PORT")
        .unwrap_or_else(|_| String::from("9090"))
        .parse()
        .expect("PORT must be a number");

    let binding_interface = format!("0.0.0.0:{port}");
    let config = ApnsConfig {
        key_id: "9F437T6Y4G".to_string(),
        team_id: "JX83D66C47".to_string(),
        private_key: std::fs::read_to_string("key.p8")?,
        sandbox: false,
    };

    HttpServer::new(move || {
        App::new()
            .app_data(Data::new(config.clone()))
            .wrap(Logger::default())
            .wrap(middleware::DefaultHeaders::new().add(("X-Version", env!("CARGO_PKG_VERSION"))))
            .service(ok)
            .service(push)
    })
    .bind(binding_interface)?
    .run()
    .await
}
