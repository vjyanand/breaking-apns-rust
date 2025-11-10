use std::{collections::HashMap, convert::Infallible};

use crate::{config::ApnsConfig, processor::ApnsProcessor};
use log::{info, warn};
use warp::{reject::Rejection, reply::Reply};

pub async fn push(query: HashMap<String, String>) -> Result<Box<dyn Reply>, Rejection> {
    let Some(news_id) = query.get("newsId") else {
        return Ok(Box::new(warp::reply::with_status("Missing newsId", warp::http::StatusCode::BAD_REQUEST)));
    };
    let Ok(news_id) = news_id.parse::<i64>() else {
        return Ok(Box::new(warp::reply::with_status("Invalid newsId", warp::http::StatusCode::BAD_REQUEST)));
    };
    let private_key = match std::fs::read_to_string("key.p8") {
        Ok(private_key) => private_key,
        Err(e) => {
            warn!("Error getting config: {e}");
            let error_msg = format!("Error getting config: {e}");
            return Ok(Box::new(warp::reply::with_status(error_msg, warp::http::StatusCode::INTERNAL_SERVER_ERROR)));
        }
    };

    let config = ApnsConfig { key_id: "9F437T6Y4G".to_owned(), team_id: "JX83D66C47".to_owned(), private_key, sandbox: false };
    let db_partition = query.get("db_partition");
    let device_hash = query.get("dhash");
    let Some(db_partition) = db_partition.and_then(|s| s.parse::<u8>().ok()) else {
        warn!("Error getting db_partition {:?}", db_partition);
        let error_msg = "Error getting db_partition".to_owned();
        return Ok(Box::new(warp::reply::with_status(error_msg, warp::http::StatusCode::INTERNAL_SERVER_ERROR)));
    };

    if let Ok(processor) = ApnsProcessor::new(&config, db_partition, 5) {
        processor.process_notifications(news_id, device_hash).await
    }

    Ok(Box::new(warp::reply::with_status("Ok", warp::http::StatusCode::OK)))
}

pub async fn ok() -> Result<impl Reply, Infallible> {
    info!("Stats endpoint called");
    Ok(warp::reply::json(&vec!["Good to see the Rust WARP App up and running"]))
}
