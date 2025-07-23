use std::borrow::Cow;

use phf::phf_map;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sqlx::{FromRow, Row, Type};
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

// Static lookup tables using phf for zero-allocation lookups
static SOURCE_NAMES: phf::Map<i64, &'static str> = phf_map! {
    1_i64 => "CNN",
    2_i64 => "WSJ",
    4_i64 => "MSNBC",
    8_i64 => "BBC",
    16_i64 => "NPR",
    32_i64 => "CNBC",
    64_i64 => "TMZ",
    128_i64 => "Boston Globe",
    256_i64 => "The Spectator Index",
    512_i64 => "Fox News",
    1024_i64 => "Fox Business",
    2048_i64 => "NYT",
    4096_i64 => "AP",
    8192_i64 => "NYP",
    16384_i64 => "CBS News",
    32768_i64 => "France 24",
    65536_i64 => "ESPN",
    131072_i64 => "ABC News",
    262144_i64 => "NBC News",
    524288_i64 => "AFP",
    1048576_i64 => "Sky News",
    2097152_i64 => "Bloomberg",
    4194304_i64 => "NigeriaStories",
    8388608_i64 => "Sky Sports",
    16777216_i64 => "People's Daily",
    33554432_i64 => "Al Jazeera",
    67108864_i64 => "Reuters",
    134217728_i64 => "XHNews",
    268435456_i64 => "Politico",
    536870912_i64 => "Times Of India",
    1073741824_i64 => "USA TODAY",
    2147483648_i64 => "HuffPost",
    4294967296_i64 => "The Hill",
    8589934592_i64 => "NFL",
    17179869184_i64 => "NHKニュース",
    34359738368_i64 => "TorontoStar",
    68719476736_i64 => "Sky News Australia",
    137438953472_i64 => "The Washington Post",
    274877906944_i64 => "Forbes",
    549755813888_i64 => "Financial Times",
    1099511627776_i64 => "ABS-CBN News",
    2199023255552_i64 => "The National",
    4398046511104_i64 => "Israel News",
    8796093022208_i64 => "NZ News",
    17592186044416_i64 => "DW News",
    35184372088832_i64 => "Newsmax",
    70368744177664_i64 => "RedState",
    140737488355328_i64 => "TheBlaze",
    281474976710656_i64 => "Agenzia ANSA",
};

static SOUND_NAMES: phf::Map<i16, &'static str> = phf_map! {
    0_i16 => "",
    1_i16 => "default",
    2_i16 => "g.caf",
    3_i16 => "p.caf",
    4_i16 => "s.caf",
    5_i16 => "gl.caf",
    6_i16 => "sm.caf",
    7_i16 => "w.caf",
};

fn get_source_name(news_id: i64) -> &'static str {
    SOURCE_NAMES.get(&news_id).copied().unwrap_or("WSJ❔")
}

fn get_sound_name(sound_id: i16) -> &'static str {
    SOUND_NAMES.get(&sound_id).copied().unwrap_or("default")
}

impl<'r> FromRow<'r, sqlx::postgres::PgRow> for PushNotification {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> Result<Self, sqlx::Error> {
        let paid: bool = row.try_get("paid")?;
        let playsound: bool = row.try_get("playsound")?;
        let sound_id: i16 = row.try_get("sound_id")?;
        let news_id: i64 = row.try_get("news_id")?;
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

        let mut custom = Map::with_capacity(2);
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
        // let collapse_id = if paid {
        //     Some(Cow::Borrowed(news_source))
        // } else {
        //     Some(Cow::Borrowed("2"))
        // };
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
