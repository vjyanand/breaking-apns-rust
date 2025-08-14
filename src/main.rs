mod apns;
mod client;
mod config;
mod handler;
mod processor;
use std::env;

use actix_web::middleware::{self, Logger};
use actix_web::{App, HttpServer};
use log::warn;

use crate::handler::{ok, push};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("warn"))
        .format_timestamp_secs() // Include timestamps in seconds
        .init();

    let port: u16 = env::var("PORT").unwrap_or_else(|_| String::from("9090")).parse().expect("PORT must be a number");

    let binding_interface = format!("0.0.0.0:{port}");

    warn!("Starting APNs server at {binding_interface}");

    HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .wrap(middleware::DefaultHeaders::new().add(("X-Version", env!("CARGO_PKG_VERSION"))))
            .service(ok)
            .service(push)
    })
    .bind(binding_interface)?
    .run()
    .await
}
