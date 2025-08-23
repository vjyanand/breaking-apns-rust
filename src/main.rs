mod apns;
mod client;
mod config;
mod handler;
mod processor;
use std::env;

use log::warn;
use warp::Filter;

#[tokio::main]
async fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("warn"))
        .format_timestamp_secs() // Include timestamps in seconds
        .init();

    let port: u16 = env::var("PORT").unwrap_or_else(|_| String::from("9090")).parse().expect("PORT must be a number");

    let stats = warp::path("stats").and(warp::get()).and_then(handler::ok);

    let push = warp::path!("push" / "ios" / "breaking")
        .and(warp::get())
        .and(warp::query::<std::collections::HashMap<String, String>>())
        .and_then(handler::push);

    // Combine routes with middleware
    let routes = stats.or(push).with(warp::log("apns_server")).with(warp::reply::with::header("X-Version", env!("CARGO_PKG_VERSION")));

    warn!("Starting APNs server at 0.0.0.0:{port}");

    warp::serve(routes).run(([0, 0, 0, 0], port)).await;
}
