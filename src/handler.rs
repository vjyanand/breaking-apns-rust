use std::{collections::HashMap, convert::Infallible};

use crate::{config::ApnsConfig, processor::ApnsProcessor};
use log::warn;
use sysinfo::{ProcessesToUpdate, System};
use warp::{reject::Rejection, reply::Reply};

pub async fn push(query: HashMap<String, String>) -> Result<Box<dyn Reply>, Rejection> {
    let news_id = match query.get("newsId") {
        Some(id) => match id.parse::<i64>() {
            Ok(id) => id,
            Err(_) => return Ok(Box::new(warp::reply::with_status("Invalid newsId", warp::http::StatusCode::BAD_REQUEST))),
        },
        None => return Ok(Box::new(warp::reply::with_status("Missing newsId", warp::http::StatusCode::BAD_REQUEST))),
    };

    let private_key = match std::fs::read_to_string("key.p8") {
        Ok(private_key) => private_key,
        Err(e) => {
            warn!("Error getting config: {e}");
            let error_msg = format!("Error getting config: {e}");
            return Ok(Box::new(warp::reply::with_status(error_msg, warp::http::StatusCode::INTERNAL_SERVER_ERROR)));
        }
    };

    let config = ApnsConfig { key_id: "9F437T6Y4G".to_string(), team_id: "JX83D66C47".to_string(), private_key, sandbox: false };
    if let Ok(processor) = ApnsProcessor::new(&config, 50).await {
        processor.pool.close().await;
    }

    Ok(Box::new(warp::reply::with_status("Ok", warp::http::StatusCode::OK)))
}

pub async fn ok() -> Result<impl Reply, Infallible> {
    Ok(warp::reply::json(&vec!["Nice to see the Test3 Rust WARP app up and running"]))
}
