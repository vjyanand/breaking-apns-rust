use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sqlx::{FromRow, Row, Type};
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ApnsPayload {
    pub aps: ApsPayload,
    #[serde(flatten)]
    pub custom: Map<String, Value>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ApsPayload {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub alert: Option<AlertPayload>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub badge: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sound: Option<String>,
    #[serde(rename = "content-available", skip_serializing_if = "Option::is_none")]
    pub content_available: Option<i32>,
}

impl Default for ApsPayload {
    fn default() -> Self {
        Self {
            alert: None,
            badge: None,
            sound: None,
            content_available: None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AlertPayload {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subtitle: Option<String>,
    pub body: String,
}

#[derive(Debug)]
pub struct PushNotification {
    pub id: Uuid,
    pub device_token: String,
    pub priority: Option<i32>,
    pub expiration: Option<u64>,
    pub collapse_id: Option<String>,
    pub push_type: BreakingApnsType,
    pub payload: ApnsPayload,
}

#[derive(Debug, Type, Serialize, Deserialize)]
#[sqlx(type_name = "breaking_apns_type", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum BreakingApnsType {
    App,
    Watch,
    Complication,
}

struct Source(String);
impl From<i64> for Source {
    fn from(news_id: i64) -> Self {
        let source = match news_id {
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
        };
        Source(source.to_string())
    }
}

struct Sound(String);
impl From<i16> for Sound {
    fn from(sound_id: i16) -> Self {
        let sound = match sound_id {
            0 => "",
            1 => "default",
            2 => "g.caf",
            3 => "p.caf",
            4 => "s.caf",
            5 => "gl.caf",
            6 => "sm.caf",
            7 => "w.caf",
            _ => "default",
        };
        Sound(sound.to_string())
    }
}

impl FromRow<'_, sqlx::postgres::PgRow> for PushNotification {
    fn from_row(row: &sqlx::postgres::PgRow) -> Result<Self, sqlx::Error> {
        let paid: bool = row.try_get("paid")?;
        let playsound: bool = row.try_get("playsound")?;
        let sound_id: i16 = row.try_get("sound_id")?;
        let news_id: i64 = row.try_get("news_id")?;
        let news_source = Source::from(news_id).0;
        let push_type: BreakingApnsType = row.try_get("type")?;
        let alert = AlertPayload {
            title: if paid {
                Some(news_source.clone())
            } else {
                None
            },
            subtitle: Some("stitle".to_string()),
            body: row.try_get("text")?,
        };
        let sound: Option<String> = if playsound {
            Some(Sound::from(sound_id).0)
        } else {
            None
        };
        let aps: ApsPayload = ApsPayload {
            alert: Some(alert),
            badge: None,
            sound,
            content_available: None,
        };
        let custom = serde_json::json!({
            "s": news_source,
            "nid": news_id,
        })
        .as_object()
        .unwrap()
        .clone();

        let payload: ApnsPayload = ApnsPayload { aps, custom };

        Ok(Self {
            id: row.try_get("id")?,
            device_token: row.try_get("token")?,
            priority: Some(10),
            expiration: None,
            collapse_id: Some("1".to_owned()),
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
