use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sqlx::{FromRow, Row, Type};
use std::borrow::Cow;
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
pub struct ApnsPayload {
    pub aps: ApsPayload,
    #[serde(flatten)]
    pub custom: Map<String, Value>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct ApsPayload {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub alert: Option<AlertPayload>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub badge: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sound: Option<Cow<'static, str>>,
    #[serde(rename = "content-available", skip_serializing_if = "Option::is_none")]
    pub content_available: Option<i32>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AlertPayload {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<Cow<'static, str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subtitle: Option<Cow<'static, str>>,
    pub body: String,
}

#[derive(Debug)]
pub struct PushNotification {
    pub id: Uuid,
    pub device_token: String,
    pub priority: Option<i32>,
    pub expiration: Option<u64>,
    pub collapse_id: Option<Cow<'static, str>>,
    pub push_type: BreakingApnsType,
    pub payload: ApnsPayload,
}

#[derive(Debug, Type, Serialize, Deserialize, PartialEq)]
#[sqlx(type_name = "breaking_apns_type", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum BreakingApnsType {
    App,
    Watch,
    Complication,
}

// Convert to const lookup for better performance
const fn get_source_name(id: i64) -> &'static str {
    match id {
        1 => "CNN",
        2 => "WSJ",
        4 => "MSNBC",
        8 => "BBC",
        16 => "NPR",
        32 => "CNBC",
        64 => "TMZ",
        128 => "Boston Globe",
        256 => "The Spectator Index",
        512 => "Fox News",
        1024 => "Fox Business",
        2048 => "NYT",
        4096 => "AP",
        8192 => "NYP",
        16384 => "CBS News",
        32768 => "France 24",
        65536 => "ESPN",
        131072 => "ABC News",
        262144 => "NBC News",
        524288 => "AFP",
        1048576 => "Sky News",
        2097152 => "Bloomberg",
        4194304 => "NigeriaStories",
        8388608 => "Sky Sports",
        16777216 => "People's Daily",
        33554432 => "Al Jazeera",
        67108864 => "Reuters",
        134217728 => "XHNews",
        268435456 => "Politico",
        536870912 => "Times Of India",
        1073741824 => "USA TODAY",
        2147483648 => "HuffPost",
        4294967296 => "The Hill",
        8589934592 => "NFL",
        17179869184 => "NHKニュース",
        34359738368 => "TorontoStar",
        68719476736 => "Sky News Australia",
        137438953472 => "The Washington Post",
        274877906944 => "Forbes",
        549755813888 => "Financial Times",
        1099511627776 => "ABS-CBN News",
        2199023255552 => "The National",
        4398046511104 => "Israel News",
        8796093022208 => "NZ News",
        17592186044416 => "DW News",
        35184372088832 => "Newsmax",
        70368744177664 => "RedState",
        140737488355328 => "TheBlaze",
        281474976710656 => "Agenzia ANSA",
        _ => "WSJ❔",
    }
}

use std::collections::HashMap;
use std::sync::OnceLock;

static SOUND_NAMES: OnceLock<HashMap<i16, &'static str>> = OnceLock::new();

fn get_sound_names_map() -> &'static HashMap<i16, &'static str> {
    SOUND_NAMES.get_or_init(|| {
        let mut map = HashMap::new();
        map.insert(0, "");
        map.insert(1, "default");
        map.insert(2, "g.caf");
        map.insert(3, "p.caf");
        map.insert(4, "s.caf");
        map.insert(5, "gl.caf");
        map.insert(6, "sm.caf");
        map.insert(7, "w.caf");
        map
    })
}

fn get_sound_name(sound_id: i16) -> &'static str {
    get_sound_names_map().get(&sound_id).unwrap_or(&"default")
}

impl<'r> FromRow<'r, sqlx::postgres::PgRow> for PushNotification {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> Result<Self, sqlx::Error> {
        let paid: bool = row.try_get("paid")?;
        let playsound: bool = row.try_get("playsound")?;
        let sound_id: i16 = row.try_get("sound_id")?;
        let news_id: i64 = row.try_get("news_id")?;

        // Direct lookup - no caching needed if news_id is the same for all rows
        let news_source = get_source_name(news_id);

        let push_type: BreakingApnsType = row.try_get("type")?;
        let news_date: i64 = row.try_get("news_date")?;

        let alert = AlertPayload {
            subtitle: if paid {
                Some(Cow::Borrowed(news_source))
            } else {
                None
            },
            title: Some(Cow::Borrowed("Breaking News")),
            body: row.try_get("text")?,
        };

        let sound = if playsound {
            let sound_name = get_sound_name(sound_id);
            if sound_name.is_empty() {
                None
            } else {
                Some(Cow::Borrowed(sound_name))
            }
        } else {
            None
        };

        let aps = ApsPayload {
            alert: Some(alert),
            badge: None,
            sound,
            content_available: None,
        };

        let mut custom = Map::with_capacity(5); // Preallocate correct capacity
        custom.insert("s".into(), Value::String(news_source.into()));
        custom.insert("nid".into(), Value::Number(news_id.into()));
        custom.insert("p".into(), Value::Bool(paid));
        custom.insert("t".into(), Value::Number(news_date.into()));

        let url = row.try_get::<String, _>("url")?;
        if url.len() < 3 {
            custom.insert(
                "_u".into(),
                Value::String(format!("https://breaking.iavian.net/article/{news_id}")),
            );
        } else {
            custom.insert("_u".into(), Value::String(url));
        }

        let payload = ApnsPayload { aps, custom };
        let collapse_id = Some(Cow::Borrowed(news_source));

        Ok(Self {
            id: row.try_get("id")?,
            device_token: row.try_get("token")?,
            priority: Some(10),
            expiration: None,
            collapse_id,
            push_type,
            payload,
        })
    }
}

#[derive(Debug)]
pub struct PushResult {
    pub notification_id: Uuid,
    pub success: bool,
    pub status_code: u16,
    pub apns_id: Option<String>,
    pub error: Option<String>,
}
