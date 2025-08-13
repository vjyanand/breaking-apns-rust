use crate::{config::ApnsConfig, processor::ApnsProcessor};
use actix_web::{HttpResponse, Responder, web};
use log::warn;
use sqlx::{Pool, Postgres};

#[actix_web::get("/push/ios/breaking")]
pub async fn push(query: web::Query<std::collections::HashMap<String, String>>) -> impl Responder {
    let news_id = match query.get("newsId") {
        Some(id) => match id.parse::<i64>() {
            Ok(id) => id,
            Err(_) => return HttpResponse::BadRequest().body("Invalid newsId"),
        },
        None => return HttpResponse::BadRequest().body("Missing newsId"),
    };
    let private_key = match std::fs::read_to_string("key.p8") {
        Ok(private_key) => private_key,
        Err(e) => {
            warn!("Error getting config: {e}");
            return HttpResponse::InternalServerError().body(format!("Error getting config: {e}"));
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
                    return HttpResponse::InternalServerError().body(format!("Error creating client: {e}"));
                }
            };
            warn!("Connected to Postgres {news_id}");
            processor.process_notifications(pool, news_id, device_hash).await;
            warn!("Finished processing notifications {news_id}");
            HttpResponse::Ok().body("Ok")
        }
        Err(err) => {
            warn!("Failed to create APNS processor: {err}");
            HttpResponse::InternalServerError().body(format!("Failed to create APNS processor: {err}"))
        }
    }
}

#[actix_web::get("/stats")]
pub async fn ok() -> impl Responder {
    HttpResponse::Ok().json(vec!["Nice to see the RUST app up and running"])
}
