use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PollStatus {
    OPEN,
    CLOSED,
    EXPIRED,
}

impl PollStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            PollStatus::OPEN => "OPEN",
            PollStatus::CLOSED => "CLOSED",
            PollStatus::EXPIRED => "EXPIRED",
        }
    }

    pub fn from_db_str(s: &str) -> Self {
        match s {
            "CLOSED" => PollStatus::CLOSED,
            "EXPIRED" => PollStatus::EXPIRED,
            _ => PollStatus::OPEN,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DurationOption {
    pub minutes: Option<i64>,
    pub label: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreatePollRequest {
    pub title: String,
    pub description: Option<String>,
    pub options: Vec<String>,
    pub password: Option<String>,
    pub duration_minutes: Option<i64>,
    #[serde(default)]
    pub captcha_required: Option<bool>,
    pub results_visibility: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CastVoteRequest {
    pub option_id: Uuid,
    pub password: Option<String>,
    pub voter_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PollOptionResponse {
    pub id: Uuid,
    pub option_text: String,
    pub display_order: i32,
    pub vote_count: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PollResponse {
    pub id: Uuid,
    pub title: String,
    pub description: Option<String>,
    pub status: PollStatus,
    pub expires_at: Option<NaiveDateTime>,
    pub has_password: Option<bool>,
    pub captcha_required: Option<bool>,
    pub results_visibility: Option<String>,
    pub created_at: Option<NaiveDateTime>,
    pub options: Option<Vec<PollOptionResponse>>,
    pub total_votes: Option<i64>,
    pub has_voted: Option<bool>,
    pub admin_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VoteUpdateMessage {
    pub poll_id: Uuid,
    pub total_votes: i64,
    pub options: Vec<PollOptionResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BatchStatusRequest {
    pub poll_ids: Vec<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PollStatusSummary {
    pub id: Uuid,
    pub title: String,
    pub status: PollStatus,
}
