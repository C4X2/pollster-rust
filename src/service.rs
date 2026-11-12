use crate::{
    config::AppConfig,
    error::AppError,
    models::{
        CastVoteRequest, CreatePollRequest, DurationOption, PollOptionResponse, PollResponse,
        PollStatus, PollStatusSummary, VoteUpdateMessage,
    },
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use chrono::{Duration, Local, NaiveDateTime};
use rand::{rngs::OsRng, RngCore};
use rusqlite::{params, Connection, OptionalExtension};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use tokio::sync::broadcast;
use uuid::Uuid;

const SALT: &str = "POLLSTER_SECRET_SALT_2026";
const DATETIME_FMT: &str = "%Y-%m-%dT%H:%M:%S%.6f";

struct DbPoll {
    id: Uuid,
    title: String,
    description: Option<String>,
    admin_key: String,
    password: Option<String>,
    status: PollStatus,
    expires_at: Option<NaiveDateTime>,
    captcha_required: bool,
    results_visibility: String,
    created_at: NaiveDateTime,
}

impl DbPoll {
    fn effective_status(&self) -> PollStatus {
        if self.status == PollStatus::CLOSED {
            return PollStatus::CLOSED;
        }
        if let Some(exp) = self.expires_at {
            if Local::now().naive_local() > exp {
                return PollStatus::EXPIRED;
            }
        }
        PollStatus::OPEN
    }
}

#[derive(Clone)]
pub struct PollService {
    db: Arc<Mutex<Connection>>,
    config: AppConfig,
    channels: Arc<Mutex<HashMap<Uuid, broadcast::Sender<VoteUpdateMessage>>>>,
}

impl PollService {
    pub fn new(conn: Connection, config: AppConfig) -> Self {
        Self {
            db: Arc::new(Mutex::new(conn)),
            config,
            channels: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn get_duration_options(&self) -> &[DurationOption] {
        &self.config.durations
    }

    pub fn is_valid_duration_option(&self, duration_minutes: Option<i64>) -> bool {
        let options = &self.config.durations;
        if options.is_empty() {
            return true;
        }
        for opt in options {
            match duration_minutes {
                None | Some(0) => {
                    if opt.minutes.is_none() || opt.minutes == Some(0) {
                        return true;
                    }
                }
                Some(min) => {
                    if opt.minutes == Some(min) {
                        return true;
                    }
                }
            }
        }
        false
    }

    pub fn create_poll(&self, request: CreatePollRequest) -> Result<PollResponse, AppError> {
        let title = request.title.trim().to_string();
        if title.len() < 3 || title.len() > 255 {
            return Err(AppError::BadRequest(
                "Title must be between 3 and 255 characters".to_string(),
            ));
        }

        let clean_options: Vec<String> = request
            .options
            .into_iter()
            .map(|o| o.trim().to_string())
            .filter(|o| !o.is_empty())
            .collect();

        if clean_options.len() < 2 || clean_options.len() > 20 {
            return Err(AppError::BadRequest(
                "Poll options must be between 2 and 20".to_string(),
            ));
        }

        if !self.is_valid_duration_option(request.duration_minutes) {
            return Err(AppError::BadRequest(format!(
                "Invalid duration option specified: {:?}",
                request.duration_minutes
            )));
        }

        let poll_id = Uuid::new_v4();
        let admin_key = generate_secure_admin_key();
        let now = Local::now().naive_local();
        let expires_at = request
            .duration_minutes
            .filter(|&m| m > 0)
            .map(|m| now + Duration::minutes(m));

        let description = request
            .description
            .map(|d| d.trim().to_string())
            .filter(|d| !d.is_empty());
        let password = request
            .password
            .filter(|p| !p.trim().is_empty());
        let captcha_required = request.captcha_required.unwrap_or(false);
        let results_visibility = request
            .results_visibility
            .filter(|v| !v.trim().is_empty())
            .unwrap_or_else(|| "PUBLIC".to_string());

        let mut option_responses = Vec::with_capacity(clean_options.len());

        {
            let mut conn = self.db.lock().unwrap();
            let tx = conn.transaction()?;

            tx.execute(
                "INSERT INTO polls (
                    id, title, description, admin_key, password, status,
                    expires_at, captcha_required, results_visibility, created_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                params![
                    poll_id.to_string(),
                    &title,
                    &description,
                    &admin_key,
                    &password,
                    PollStatus::OPEN.as_str(),
                    expires_at.map(|dt| dt.format(DATETIME_FMT).to_string()),
                    captcha_required as i32,
                    &results_visibility,
                    now.format(DATETIME_FMT).to_string(),
                ],
            )?;

            {
                let mut stmt = tx.prepare_cached(
                    "INSERT INTO poll_options (id, poll_id, option_text, display_order, vote_count)
                     VALUES (?1, ?2, ?3, ?4, 0)",
                )?;

                for (idx, opt_text) in clean_options.into_iter().enumerate() {
                    let opt_id = Uuid::new_v4();
                    let order = idx as i32;
                    stmt.execute(params![
                        opt_id.to_string(),
                        poll_id.to_string(),
                        &opt_text,
                        order,
                    ])?;
                    option_responses.push(PollOptionResponse {
                        id: opt_id,
                        option_text: opt_text,
                        display_order: order,
                        vote_count: 0,
                    });
                }
            }

            tx.commit()?;
        }

        let poll = DbPoll {
            id: poll_id,
            title,
            description,
            admin_key: admin_key.clone(),
            password,
            status: PollStatus::OPEN,
            expires_at,
            captcha_required,
            results_visibility,
            created_at: now,
        };

        Ok(map_to_poll_response(
            &poll,
            option_responses,
            Some(admin_key),
            false,
        ))
    }

    pub fn get_poll(
        &self,
        poll_id: Uuid,
        password: Option<&str>,
        voter_ip: Option<&str>,
        voter_token: Option<&str>,
        admin_key: Option<&str>,
    ) -> Result<PollResponse, AppError> {
        let voter_hash = compute_voter_hash(poll_id, voter_ip, voter_token);

        let conn = self.db.lock().unwrap();
        let poll = load_poll(&conn, poll_id)?
            .ok_or_else(|| AppError::BadRequest(format!("Poll not found with ID: {poll_id}")))?;

        let is_admin = admin_key.is_some_and(|k| k == poll.admin_key);

        if let Some(ref poll_pw) = poll.password {
            if !is_admin && password != Some(poll_pw.as_str()) {
                return Ok(PollResponse {
                    id: poll.id,
                    title: poll.title.clone(),
                    description: None,
                    status: poll.effective_status(),
                    expires_at: None,
                    has_password: Some(true),
                    captcha_required: None,
                    results_visibility: None,
                    created_at: None,
                    options: None,
                    total_votes: None,
                    has_voted: None,
                    admin_key: None,
                });
            }
        }

        let has_voted: bool = conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM votes WHERE poll_id = ?1 AND voter_hash = ?2)",
            params![poll_id.to_string(), voter_hash],
            |row| row.get(0),
        )?;

        let options = load_options(&conn, poll_id)?;
        let returned_admin_key = if is_admin {
            Some(poll.admin_key.clone())
        } else {
            None
        };

        Ok(map_to_poll_response(
            &poll,
            options,
            returned_admin_key,
            has_voted,
        ))
    }

    pub fn cast_vote(
        &self,
        poll_id: Uuid,
        request: CastVoteRequest,
        voter_ip: Option<&str>,
    ) -> Result<PollResponse, AppError> {
        let voter_hash = compute_voter_hash(poll_id, voter_ip, request.voter_token.as_deref());

        let (poll, options) = {
            let mut conn = self.db.lock().unwrap();
            let tx = conn.transaction()?;

            let poll = load_poll(&tx, poll_id)?
                .ok_or_else(|| AppError::BadRequest(format!("Poll not found with ID: {poll_id}")))?;

            match poll.effective_status() {
                PollStatus::CLOSED => {
                    return Err(AppError::BadRequest(
                        "This poll has been closed by the creator.".to_string(),
                    ));
                }
                PollStatus::EXPIRED => {
                    return Err(AppError::BadRequest("This poll has expired.".to_string()));
                }
                PollStatus::OPEN => {}
            }

            if let Some(ref poll_pw) = poll.password {
                if request.password.as_deref() != Some(poll_pw.as_str()) {
                    return Err(AppError::BadRequest(
                        "Invalid password for protected poll.".to_string(),
                    ));
                }
            }

            let already_voted: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM votes WHERE poll_id = ?1 AND voter_hash = ?2)",
                params![poll_id.to_string(), &voter_hash],
                |row| row.get(0),
            )?;

            if already_voted {
                return Err(AppError::BadRequest(
                    "You have already voted in this poll.".to_string(),
                ));
            }

            let option_poll_id: Option<String> = tx
                .query_row(
                    "SELECT poll_id FROM poll_options WHERE id = ?1",
                    params![request.option_id.to_string()],
                    |row| row.get(0),
                )
                .optional()?;

            match option_poll_id {
                None => {
                    return Err(AppError::BadRequest("Invalid option selected.".to_string()));
                }
                Some(ref owner_id) if owner_id != &poll_id.to_string() => {
                    return Err(AppError::BadRequest(
                        "Option does not belong to this poll.".to_string(),
                    ));
                }
                Some(_) => {}
            }

            let vote_id = Uuid::new_v4();
            let now = Local::now().naive_local();
            tx.execute(
                "INSERT INTO votes (id, poll_id, option_id, voter_hash, voted_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    vote_id.to_string(),
                    poll_id.to_string(),
                    request.option_id.to_string(),
                    &voter_hash,
                    now.format(DATETIME_FMT).to_string(),
                ],
            )?;

            tx.execute(
                "UPDATE poll_options SET vote_count = vote_count + 1 WHERE id = ?1",
                params![request.option_id.to_string()],
            )?;

            let options = load_options(&tx, poll_id)?;
            tx.commit()?;

            (poll, options)
        };

        self.broadcast_vote_update(poll_id, &options);

        Ok(map_to_poll_response(&poll, options, None, true))
    }

    pub fn close_poll(
        &self,
        poll_id: Uuid,
        admin_key: Option<&str>,
    ) -> Result<PollResponse, AppError> {
        self.set_poll_status(poll_id, admin_key, PollStatus::CLOSED)
    }

    pub fn reopen_poll(
        &self,
        poll_id: Uuid,
        admin_key: Option<&str>,
    ) -> Result<PollResponse, AppError> {
        self.set_poll_status(poll_id, admin_key, PollStatus::OPEN)
    }

    fn set_poll_status(
        &self,
        poll_id: Uuid,
        admin_key: Option<&str>,
        new_status: PollStatus,
    ) -> Result<PollResponse, AppError> {
        let key = admin_key
            .filter(|k| !k.trim().is_empty())
            .ok_or_else(|| AppError::Forbidden("Invalid admin key or poll not found.".to_string()))?;

        let (mut poll, options) = {
            let conn = self.db.lock().unwrap();
            let poll = load_poll(&conn, poll_id)?
                .filter(|p| p.admin_key == key)
                .ok_or_else(|| {
                    AppError::Forbidden("Invalid admin key or poll not found.".to_string())
                })?;

            conn.execute(
                "UPDATE polls SET status = ?1 WHERE id = ?2",
                params![new_status.as_str(), poll_id.to_string()],
            )?;

            let options = load_options(&conn, poll_id)?;
            (poll, options)
        };

        poll.status = new_status;
        self.broadcast_vote_update(poll_id, &options);

        Ok(map_to_poll_response(
            &poll,
            options,
            Some(key.to_string()),
            false,
        ))
    }

    pub fn delete_poll(&self, poll_id: Uuid, admin_key: Option<&str>) -> Result<(), AppError> {
        let key = admin_key
            .filter(|k| !k.trim().is_empty())
            .ok_or_else(|| AppError::Forbidden("Invalid admin key or poll not found.".to_string()))?;

        {
            let conn = self.db.lock().unwrap();
            let deleted = conn.execute(
                "DELETE FROM polls WHERE id = ?1 AND admin_key = ?2",
                params![poll_id.to_string(), key],
            )?;

            if deleted == 0 {
                return Err(AppError::Forbidden(
                    "Invalid admin key or poll not found.".to_string(),
                ));
            }
        }

        self.channels.lock().unwrap().remove(&poll_id);
        Ok(())
    }

    pub fn get_batch_statuses(
        &self,
        poll_ids: &[Uuid],
    ) -> Result<Vec<PollStatusSummary>, AppError> {
        if poll_ids.is_empty() {
            return Ok(Vec::new());
        }

        let conn = self.db.lock().unwrap();
        let mut stmt = conn.prepare_cached(
            "SELECT id, title, status, expires_at FROM polls WHERE id = ?1",
        )?;

        let mut summaries = Vec::with_capacity(poll_ids.len());
        let now = Local::now().naive_local();

        for id in poll_ids.iter().take(50) {
            if let Some(summary) = stmt
                .query_row(params![id.to_string()], |row| {
                    let title: String = row.get(1)?;
                    let status_str: String = row.get(2)?;
                    let expires_str: Option<String> = row.get(3)?;
                    let stored_status = PollStatus::from_db_str(&status_str);
                    let expires_at = expires_str.as_deref().and_then(parse_datetime);

                    let effective = if stored_status == PollStatus::CLOSED {
                        PollStatus::CLOSED
                    } else if expires_at.is_some_and(|exp| now > exp) {
                        PollStatus::EXPIRED
                    } else {
                        PollStatus::OPEN
                    };

                    Ok(PollStatusSummary {
                        id: *id,
                        title,
                        status: effective,
                    })
                })
                .optional()?
            {
                summaries.push(summary);
            }
        }

        Ok(summaries)
    }

    pub fn subscribe(&self, poll_id: Uuid) -> broadcast::Receiver<VoteUpdateMessage> {
        let mut channels = self.channels.lock().unwrap();
        let sender = channels
            .entry(poll_id)
            .or_insert_with(|| broadcast::channel(32).0);
        sender.subscribe()
    }

    fn broadcast_vote_update(&self, poll_id: Uuid, options: &[PollOptionResponse]) {
        let mut channels = self.channels.lock().unwrap();
        if let Some(sender) = channels.get(&poll_id) {
            if sender.receiver_count() == 0 {
                channels.remove(&poll_id);
                return;
            }
            let total_votes: i64 = options.iter().map(|o| o.vote_count).sum();
            let msg = VoteUpdateMessage {
                poll_id,
                total_votes,
                options: options.to_vec(),
            };
            let _ = sender.send(msg);
        }
    }
}

fn load_poll(conn: &Connection, poll_id: Uuid) -> Result<Option<DbPoll>, rusqlite::Error> {
    let mut stmt = conn.prepare_cached(
        "SELECT id, title, description, admin_key, password, status,
                expires_at, captcha_required, results_visibility, created_at
         FROM polls WHERE id = ?1",
    )?;

    stmt.query_row(params![poll_id.to_string()], |row| {
        let id_str: String = row.get(0)?;
        let expires_str: Option<String> = row.get(6)?;
        let created_str: String = row.get(9)?;
        let status_str: String = row.get(5)?;
        let captcha_int: i32 = row.get(7)?;

        Ok(DbPoll {
            id: Uuid::parse_str(&id_str).unwrap_or(poll_id),
            title: row.get(1)?,
            description: row.get(2)?,
            admin_key: row.get(3)?,
            password: row.get(4)?,
            status: PollStatus::from_db_str(&status_str),
            expires_at: expires_str.as_deref().and_then(parse_datetime),
            captcha_required: captcha_int != 0,
            results_visibility: row.get(8)?,
            created_at: parse_datetime(&created_str)
                .unwrap_or_else(|| Local::now().naive_local()),
        })
    })
    .optional()
}

fn load_options(
    conn: &Connection,
    poll_id: Uuid,
) -> Result<Vec<PollOptionResponse>, rusqlite::Error> {
    let mut stmt = conn.prepare_cached(
        "SELECT id, option_text, display_order, vote_count
         FROM poll_options
         WHERE poll_id = ?1
         ORDER BY display_order ASC",
    )?;

    let rows = stmt.query_map(params![poll_id.to_string()], |row| {
        let id_str: String = row.get(0)?;
        Ok(PollOptionResponse {
            id: Uuid::parse_str(&id_str).unwrap_or_default(),
            option_text: row.get(1)?,
            display_order: row.get(2)?,
            vote_count: row.get(3)?,
        })
    })?;

    rows.collect()
}

fn parse_datetime(s: &str) -> Option<NaiveDateTime> {
    NaiveDateTime::parse_from_str(s, DATETIME_FMT)
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f"))
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S"))
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f"))
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S"))
        .ok()
}

fn map_to_poll_response(
    poll: &DbPoll,
    options: Vec<PollOptionResponse>,
    admin_key: Option<String>,
    has_voted: bool,
) -> PollResponse {
    let total_votes: i64 = options.iter().map(|o| o.vote_count).sum();
    let effective_status = poll.effective_status();

    let show_results = poll.results_visibility == "PUBLIC"
        || (effective_status == PollStatus::CLOSED
            && poll.results_visibility == "HIDDEN_UNTIL_CLOSED")
        || admin_key.is_some();

    let mapped_options = options
        .into_iter()
        .map(|mut opt| {
            if !show_results {
                opt.vote_count = 0;
            }
            opt
        })
        .collect();

    PollResponse {
        id: poll.id,
        title: poll.title.clone(),
        description: poll.description.clone(),
        status: effective_status,
        expires_at: poll.expires_at,
        has_password: Some(poll.password.is_some()),
        captcha_required: Some(poll.captcha_required),
        results_visibility: Some(poll.results_visibility.clone()),
        created_at: Some(poll.created_at),
        options: Some(mapped_options),
        total_votes: Some(if show_results { total_votes } else { 0 }),
        has_voted: Some(has_voted),
        admin_key,
    }
}

fn generate_secure_admin_key() -> String {
    let mut bytes = [0u8; 24];
    OsRng.fill_bytes(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
}

fn compute_voter_hash(
    poll_id: Uuid,
    voter_ip: Option<&str>,
    voter_token: Option<&str>,
) -> String {
    let ip = voter_ip.filter(|s| !s.is_empty()).unwrap_or("unknown");
    let token = voter_token.filter(|s| !s.is_empty()).unwrap_or("anon");
    let raw = format!("{poll_id}:{ip}:{token}:{SALT}");

    let mut hasher = Sha256::new();
    hasher.update(raw.as_bytes());
    let digest = hasher.finalize();

    let mut hex = String::with_capacity(64);
    for b in digest {
        use std::fmt::Write;
        let _ = write!(&mut hex, "{b:02x}");
    }
    hex
}
