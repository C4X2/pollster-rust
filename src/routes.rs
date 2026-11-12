use crate::{
    error::AppError,
    models::{
        BatchStatusRequest, CastVoteRequest, CreatePollRequest, DurationOption, PollResponse,
        PollStatusSummary,
    },
    service::PollService,
};
use axum::{
    extract::{ConnectInfo, Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::sse::{Event, KeepAlive, Sse},
    routing::{get, post},
    Json, Router,
};
use futures_util::stream::Stream;
use serde::Deserialize;
use serde_json::{json, Value};
use std::{convert::Infallible, net::SocketAddr, time::Duration};
use tokio_stream::{wrappers::BroadcastStream, StreamExt};
use tower_http::{cors::CorsLayer, services::ServeDir};
use uuid::Uuid;

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetPollQuery {
    pub password: Option<String>,
    pub voter_token: Option<String>,
    pub admin_key: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AdminKeyQuery {
    pub admin_key: Option<String>,
}

pub fn build_router(service: PollService) -> Router {
    let api_routes = Router::new()
        .route("/config", get(get_config))
        .route("/durations", get(get_durations))
        .route("/", post(create_poll))
        .route("/statuses", post(get_batch_statuses))
        .route("/:id", get(get_poll).delete(delete_poll))
        .route("/:id/vote", post(cast_vote))
        .route("/:id/close", post(close_poll))
        .route("/:id/reopen", post(reopen_poll))
        .route("/:id/stream", get(stream_poll_updates));

    Router::new()
        .nest("/api/polls", api_routes)
        .fallback_service(ServeDir::new("static").append_index_html_on_directories(true))
        .layer(CorsLayer::permissive())
        .with_state(service)
}

async fn get_config(State(service): State<PollService>) -> Json<Value> {
    Json(json!({
        "durations": service.get_duration_options()
    }))
}

async fn get_durations(State(service): State<PollService>) -> Json<Vec<DurationOption>> {
    Json(service.get_duration_options().to_vec())
}

async fn create_poll(
    State(service): State<PollService>,
    Json(request): Json<CreatePollRequest>,
) -> Result<(StatusCode, Json<PollResponse>), AppError> {
    let response = service.create_poll(request)?;
    Ok((StatusCode::CREATED, Json(response)))
}

async fn get_batch_statuses(
    State(service): State<PollService>,
    Json(request): Json<BatchStatusRequest>,
) -> Result<Json<Vec<PollStatusSummary>>, AppError> {
    let summaries = service.get_batch_statuses(&request.poll_ids)?;
    Ok(Json(summaries))
}

async fn get_poll(
    State(service): State<PollService>,
    Path(id): Path<Uuid>,
    Query(query): Query<GetPollQuery>,
    headers: HeaderMap,
    connect_info: Option<ConnectInfo<SocketAddr>>,
) -> Result<Json<PollResponse>, AppError> {
    let effective_admin_key = resolve_admin_key(&headers, query.admin_key.as_deref());
    let voter_ip = extract_client_ip(&headers, connect_info);

    let poll = service.get_poll(
        id,
        query.password.as_deref(),
        Some(&voter_ip),
        query.voter_token.as_deref(),
        effective_admin_key.as_deref(),
    )?;

    Ok(Json(poll))
}

async fn cast_vote(
    State(service): State<PollService>,
    Path(id): Path<Uuid>,
    headers: HeaderMap,
    connect_info: Option<ConnectInfo<SocketAddr>>,
    Json(request): Json<CastVoteRequest>,
) -> Result<Json<PollResponse>, AppError> {
    let voter_ip = extract_client_ip(&headers, connect_info);
    let response = service.cast_vote(id, request, Some(&voter_ip))?;
    Ok(Json(response))
}

async fn close_poll(
    State(service): State<PollService>,
    Path(id): Path<Uuid>,
    Query(query): Query<AdminKeyQuery>,
    headers: HeaderMap,
) -> Result<Json<PollResponse>, AppError> {
    let effective_admin_key = resolve_admin_key(&headers, query.admin_key.as_deref());
    let response = service.close_poll(id, effective_admin_key.as_deref())?;
    Ok(Json(response))
}

async fn reopen_poll(
    State(service): State<PollService>,
    Path(id): Path<Uuid>,
    Query(query): Query<AdminKeyQuery>,
    headers: HeaderMap,
) -> Result<Json<PollResponse>, AppError> {
    let effective_admin_key = resolve_admin_key(&headers, query.admin_key.as_deref());
    let response = service.reopen_poll(id, effective_admin_key.as_deref())?;
    Ok(Json(response))
}

async fn delete_poll(
    State(service): State<PollService>,
    Path(id): Path<Uuid>,
    Query(query): Query<AdminKeyQuery>,
    headers: HeaderMap,
) -> Result<Json<Value>, AppError> {
    let effective_admin_key = resolve_admin_key(&headers, query.admin_key.as_deref());
    service.delete_poll(id, effective_admin_key.as_deref())?;
    Ok(Json(json!({ "message": "Poll deleted successfully." })))
}

async fn stream_poll_updates(
    State(service): State<PollService>,
    Path(id): Path<Uuid>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let rx = service.subscribe(id);
    let stream = BroadcastStream::new(rx).filter_map(|msg| match msg {
        Ok(update) => {
            let payload = serde_json::to_string(&update).ok()?;
            Some(Ok(Event::default().data(payload)))
        }
        Err(_) => None,
    });

    Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(25))
            .text("keep-alive"),
    )
}

fn resolve_admin_key(headers: &HeaderMap, param_key: Option<&str>) -> Option<String> {
    if let Some(header_val) = headers.get("X-Admin-Key") {
        if let Ok(s) = header_val.to_str() {
            let trimmed = s.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }
    param_key
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(String::from)
}

fn extract_client_ip(
    headers: &HeaderMap,
    connect_info: Option<ConnectInfo<SocketAddr>>,
) -> String {
    if let Some(xff) = headers.get("X-Forwarded-For") {
        if let Ok(s) = xff.to_str() {
            if let Some(first) = s.split(',').next() {
                let trimmed = first.trim();
                if !trimmed.is_empty() {
                    return trimmed.to_string();
                }
            }
        }
    }
    if let Some(ConnectInfo(addr)) = connect_info {
        return addr.ip().to_string();
    }
    "127.0.0.1".to_string()
}
