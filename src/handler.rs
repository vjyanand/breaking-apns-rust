use std::{collections::HashMap, convert::Infallible};

use crate::{config::ApnsConfig, processor::ApnsProcessor};
use log::warn;
use sqlx::{Pool, Postgres};
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

    match ApnsProcessor::new(&config, 50) {
        Ok(processor) => {
            let device_hash = query.get("deviceHash");
            warn!("Connecting to Postgres {news_id}");

            let pool = match Pool::<Postgres>::connect("postgres://breaking:qwertY123@db.iavian.net/breaking").await {
                Ok(pool) => pool,
                Err(e) => {
                    let error_msg = format!("Error creating client: {e}");
                    return Ok(Box::new(warp::reply::with_status(error_msg, warp::http::StatusCode::INTERNAL_SERVER_ERROR)));
                }
            };

            warn!("Connected to Postgres {news_id}");
            processor.process_notifications(pool, news_id, device_hash).await;
            drop(processor);
            warn!("Finished processing notifications {news_id}");

            Ok(Box::new(warp::reply::with_status("Ok", warp::http::StatusCode::OK)))
        }
        Err(err) => {
            warn!("Failed to create APNS processor: {err}");
            let error_msg = format!("Failed to create APNS processor: {err}");
            Ok(Box::new(warp::reply::with_status(error_msg, warp::http::StatusCode::INTERNAL_SERVER_ERROR)))
        }
    }
}

pub async fn ok() -> Result<impl Reply, Infallible> {
    Ok(warp::reply::json(&vec!["Nice to see the RUST WARP app up and running"]))
}
