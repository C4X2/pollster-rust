pub mod config;
pub mod db;
pub mod error;
pub mod models;
pub mod routes;
pub mod service;

use config::AppConfig;
use service::PollService;
use std::net::SocketAddr;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let config = AppConfig::from_env();
    let conn = db::init_db(&config.database_path).expect("Failed to initialize SQLite database");
    let poll_service = PollService::new(conn, config.clone());

    let app = routes::build_router(poll_service);

    let addr = SocketAddr::from(([0, 0, 0, 0], config.port));
    info!(
        "Pollster (Rust) listening on http://{} (DB: {})",
        addr, config.database_path
    );

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("Failed to bind TCP listener");

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal())
    .await
    .expect("Server error");
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    info!("Shutting down Pollster server...");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::AppError;
    use crate::models::{CastVoteRequest, CreatePollRequest, PollStatus};

    fn test_service() -> PollService {
        let config = AppConfig::default();
        let conn = db::init_db(":memory:").expect("in-memory db");
        PollService::new(conn, config)
    }

    #[test]
    fn test_create_poll_and_vote_flow() {
        let service = test_service();

        // 1. Create Poll
        let request = CreatePollRequest {
            title: "Which framework do you prefer?".to_string(),
            description: Some("A quick anonymous poll for developers.".to_string()),
            options: vec![
                "Rust (Axum)".to_string(),
                "Spring Boot".to_string(),
                "FastAPI".to_string(),
            ],
            password: None,
            duration_minutes: None,
            captcha_required: Some(false),
            results_visibility: Some("PUBLIC".to_string()),
        };

        let created = service.create_poll(request).expect("create poll");
        assert!(created.admin_key.is_some());
        let options = created.options.as_ref().expect("options present");
        assert_eq!(options.len(), 3);
        assert_eq!(created.total_votes, Some(0));
        assert_eq!(created.status, PollStatus::OPEN);

        // 2. Cast Vote
        let vote_req = CastVoteRequest {
            option_id: options[0].id,
            password: None,
            voter_token: Some("test-voter-token-1".to_string()),
        };

        let voted = service
            .cast_vote(created.id, vote_req.clone(), Some("192.168.1.100"))
            .expect("cast vote");
        assert_eq!(voted.total_votes, Some(1));
        assert_eq!(voted.has_voted, Some(true));

        // 3. Attempt Duplicate Vote (Should fail)
        let dup_err = service
            .cast_vote(created.id, vote_req, Some("192.168.1.100"))
            .unwrap_err();
        assert_eq!(
            dup_err,
            AppError::BadRequest("You have already voted in this poll.".to_string())
        );

        // 4. Close Poll with Admin Key
        let admin_key = created.admin_key.as_deref();
        let closed = service
            .close_poll(created.id, admin_key)
            .expect("close poll");
        assert_eq!(closed.status, PollStatus::CLOSED);

        // 5. Attempt Vote on Closed Poll (Should fail)
        let vote_req2 = CastVoteRequest {
            option_id: options[1].id,
            password: None,
            voter_token: Some("test-voter-token-2".to_string()),
        };
        let closed_err = service
            .cast_vote(created.id, vote_req2, Some("192.168.1.101"))
            .unwrap_err();
        assert_eq!(
            closed_err,
            AppError::BadRequest("This poll has been closed by the creator.".to_string())
        );
    }

    #[test]
    fn test_invalid_admin_key() {
        let service = test_service();

        let request = CreatePollRequest {
            title: "Security test poll".to_string(),
            description: None,
            options: vec!["Option A".to_string(), "Option B".to_string()],
            password: None,
            duration_minutes: None,
            captcha_required: None,
            results_visibility: None,
        };

        let created = service.create_poll(request).expect("create poll");
        let err = service
            .close_poll(created.id, Some("invalid-key-123"))
            .unwrap_err();
        assert_eq!(
            err,
            AppError::Forbidden("Invalid admin key or poll not found.".to_string())
        );
    }

    #[test]
    fn test_duration_config_and_validation() {
        let service = test_service();
        let options = service.get_duration_options();
        assert!(!options.is_empty());

        // Valid duration (60 minutes)
        let valid_req = CreatePollRequest {
            title: "Valid duration poll".to_string(),
            description: None,
            options: vec!["Yes".to_string(), "No".to_string()],
            password: None,
            duration_minutes: Some(60),
            captcha_required: None,
            results_visibility: None,
        };
        let valid_poll = service.create_poll(valid_req).expect("valid poll");
        assert!(valid_poll.expires_at.is_some());

        // Invalid duration (9999 minutes)
        let invalid_req = CreatePollRequest {
            title: "Invalid duration poll".to_string(),
            description: None,
            options: vec!["Yes".to_string(), "No".to_string()],
            password: None,
            duration_minutes: Some(9999),
            captcha_required: None,
            results_visibility: None,
        };
        let err = service.create_poll(invalid_req).unwrap_err();
        assert!(matches!(err, AppError::BadRequest(_)));
    }

    #[tokio::test]
    async fn test_http_endpoints_end_to_end() {
        use axum::body::Body;
        use axum::http::{Request, StatusCode};
        use http_body_util::BodyExt;
        use tower::ServiceExt;

        let service = test_service();
        let app = routes::build_router(service);

        // 1. POST /api/polls
        let create_body = serde_json::json!({
            "title": "HTTP Integration Poll",
            "description": "Testing Axum routes",
            "options": ["Option 1", "Option 2"]
        });

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/polls")
                    .header("Content-Type", "application/json")
                    .body(Body::from(create_body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
        let created: models::PollResponse = serde_json::from_slice(&body_bytes).unwrap();
        let admin_key = created.admin_key.clone().unwrap();
        let option_id = created.options.as_ref().unwrap()[0].id;

        // 2. POST /api/polls/{id}/vote
        let vote_body = serde_json::json!({
            "optionId": option_id,
            "voterToken": "http-voter-1"
        });
        let vote_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/polls/{}/vote", created.id))
                    .header("Content-Type", "application/json")
                    .header("X-Forwarded-For", "10.0.0.1")
                    .body(Body::from(vote_body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(vote_res.status(), StatusCode::OK);

        // 3. POST /api/polls/statuses (batch status endpoint)
        let batch_body = serde_json::json!({
            "pollIds": [created.id]
        });
        let batch_res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/polls/statuses")
                    .header("Content-Type", "application/json")
                    .body(Body::from(batch_body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(batch_res.status(), StatusCode::OK);

        // 4. DELETE /api/polls/{id} with X-Admin-Key
        let del_res = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(format!("/api/polls/{}", created.id))
                    .header("X-Admin-Key", admin_key)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(del_res.status(), StatusCode::OK);
    }
}
