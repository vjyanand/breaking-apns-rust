use actix_web::{HttpResponse, Responder, web};
use sqlx::{Pool, Postgres};

use crate::{client::ApnsClient, config::ApnsConfig, processor::ApnsProcessor};

#[actix_web::get("/")]
pub async fn push(
    config: web::Data<ApnsConfig>,
    query: web::Query<std::collections::HashMap<String, String>>,
) -> impl Responder {
    let news_id = match query.get("newsId") {
        Some(id) => match id.parse::<i64>() {
            Ok(id) => id,
            Err(_) => return HttpResponse::BadRequest().body("Invalid newsId"),
        },
        None => return HttpResponse::BadRequest().body("Missing newsId"),
    };
    let device_hash = query.get("deviceHash");

    let client = match ApnsClient::new(&config) {
        Ok(client) => client,
        Err(e) => {
            return HttpResponse::InternalServerError().body(format!("Error creating client: {e}"));
        }
    };
    let pool =
        match Pool::<Postgres>::connect("postgres://breaking:qwertY123@db.iavian.net/breaking")
            .await
        {
            Ok(pool) => pool,
            Err(e) => {
                return HttpResponse::InternalServerError()
                    .body(format!("Error creating client: {e}"));
            }
        };
    let processor = ApnsProcessor::new(client, 4000);
    // Connect to Postgres

    //let device_hash = Some("B3C1E811-AF76-4E98-BED0-5F7D63B034B9".to_owned());

    processor
        .process_notifications(pool, news_id, device_hash)
        .await;

    HttpResponse::Ok().body("Ok")
}

#[actix_web::get("/stats")]
pub async fn ok() -> impl Responder {
    HttpResponse::Ok().json(vec!["Nice to see the script up and running"])
}
