//! REST API endpoints for transcript retrieval and management
//! 
//! This module provides HTTP endpoints for fetching complete transcripts,
//! managing sessions, and retrieving transcript metadata.

use crate::transcript::{TranscriptItem, TranscriptError, DbTranscriptSession, DbTranscriptChunk};
use crate::transcript_service::TranscriptService;
use crate::s3_service::S3Service;
use crate::db_service::DbService;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Json, Response},
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// Shared state for transcript API endpoints
#[derive(Clone)]
pub struct TranscriptApiState {
    pub transcript_service: Arc<TranscriptService>,
    pub s3_service: Arc<S3Service>,
    pub db_service: Arc<DbService>,
}

/// Query parameters for getting user sessions
#[derive(Deserialize)]
pub struct GetSessionsQuery {
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

/// Response for complete transcript retrieval
#[derive(Serialize)]
pub struct CompleteTranscriptResponse {
    pub session_id: Uuid,
    pub session_info: DbTranscriptSession,
    pub transcript_items: Vec<TranscriptItem>,
    pub total_items: usize,
    pub total_chunks: usize,
    pub total_size_bytes: i64,
}

/// Response for session list
#[derive(Serialize)]
pub struct SessionListResponse {
    pub sessions: Vec<SessionSummary>,
    pub total_count: usize,
    pub limit: i64,
    pub offset: i64,
}

/// Summary information for a transcript session
#[derive(Serialize)]
pub struct SessionSummary {
    pub session_id: Uuid,
    pub user_id: String,
    pub session_name: Option<String>,
    pub status: String,
    pub started_at: DateTime<Utc>,
    pub ended_at: Option<DateTime<Utc>>,
    pub total_chunks: usize,
    pub total_items: i32,
    pub total_size_bytes: i64,
    pub duration_minutes: Option<f64>,
}

/// Error response format
#[derive(Serialize)]
pub struct ErrorResponse {
    pub error: String,
    pub message: String,
}

/// Get complete transcript for a session
pub async fn get_complete_transcript(
    Path(session_id): Path<Uuid>,
    State(state): State<TranscriptApiState>,
) -> Response {
    info!("Fetching complete transcript for session: {}", session_id);

    match fetch_complete_transcript_internal(session_id, &state).await {
        Ok(response) => {
            info!(
                "Successfully retrieved transcript for session {}: {} items from {} chunks",
                session_id, response.total_items, response.total_chunks
            );
            (StatusCode::OK, Json(response)).into_response()
        }
        Err(e) => {
            error!("Failed to fetch transcript for session {}: {}", session_id, e);
            let error_response = ErrorResponse {
                error: "transcript_fetch_failed".to_string(),
                message: e.to_string(),
            };
            (StatusCode::INTERNAL_SERVER_ERROR, Json(error_response)).into_response()
        }
    }
}

/// Get sessions for a user
pub async fn get_user_sessions(
    Path(user_id): Path<String>,
    Query(params): Query<GetSessionsQuery>,
    State(state): State<TranscriptApiState>,
) -> Response {
    info!("Fetching sessions for user: {}", user_id);

    let limit = params.limit.unwrap_or(50);
    let offset = params.offset.unwrap_or(0);

    match get_user_sessions_internal(&user_id, limit, offset, &state).await {
        Ok(response) => {
            info!(
                "Successfully retrieved {} sessions for user {}",
                response.sessions.len(), user_id
            );
            (StatusCode::OK, Json(response)).into_response()
        }
        Err(e) => {
            error!("Failed to fetch sessions for user {}: {}", user_id, e);
            let error_response = ErrorResponse {
                error: "sessions_fetch_failed".to_string(),
                message: e.to_string(),
            };
            (StatusCode::INTERNAL_SERVER_ERROR, Json(error_response)).into_response()
        }
    }
}

/// Get session metadata
pub async fn get_session_metadata(
    Path(session_id): Path<Uuid>,
    State(state): State<TranscriptApiState>,
) -> Response {
    info!("Fetching metadata for session: {}", session_id);

    match state.db_service.get_session(session_id).await {
        Ok(Some(session)) => {
            match state.db_service.get_session_chunks(session_id).await {
                Ok(chunks) => {
                    let summary = create_session_summary(session, chunks);
                    info!("Successfully retrieved metadata for session {}", session_id);
                    (StatusCode::OK, Json(summary)).into_response()
                }
                Err(e) => {
                    error!("Failed to fetch chunks for session {}: {}", session_id, e);
                    let error_response = ErrorResponse {
                        error: "chunks_fetch_failed".to_string(),
                        message: e.to_string(),
                    };
                    (StatusCode::INTERNAL_SERVER_ERROR, Json(error_response)).into_response()
                }
            }
        }
        Ok(None) => {
            warn!("Session not found: {}", session_id);
            let error_response = ErrorResponse {
                error: "session_not_found".to_string(),
                message: format!("Session {} not found", session_id),
            };
            (StatusCode::NOT_FOUND, Json(error_response)).into_response()
        }
        Err(e) => {
            error!("Database error fetching session {}: {}", session_id, e);
            let error_response = ErrorResponse {
                error: "database_error".to_string(),
                message: e.to_string(),
            };
            (StatusCode::INTERNAL_SERVER_ERROR, Json(error_response)).into_response()
        }
    }
}

/// Delete a session and all its data
pub async fn delete_session(
    Path(session_id): Path<Uuid>,
    State(state): State<TranscriptApiState>,
) -> Response {
    info!("Deleting session: {}", session_id);

    // First get all chunks to delete from S3
    match state.db_service.get_session_chunks(session_id).await {
        Ok(chunks) => {
            // Delete S3 objects (best effort - don't fail if some are missing)
            for chunk in &chunks {
                if let Err(e) = delete_s3_chunk(&state.s3_service, &chunk.s3_key).await {
                    warn!("Failed to delete S3 chunk {}: {}", chunk.s3_key, e);
                }
            }

            // Delete from database
            match state.db_service.delete_session(session_id).await {
                Ok(_) => {
                    info!("Successfully deleted session {} and {} chunks", session_id, chunks.len());
                    (StatusCode::NO_CONTENT, ()).into_response()
                }
                Err(e) => {
                    error!("Failed to delete session {} from database: {}", session_id, e);
                    let error_response = ErrorResponse {
                        error: "delete_failed".to_string(),
                        message: e.to_string(),
                    };
                    (StatusCode::INTERNAL_SERVER_ERROR, Json(error_response)).into_response()
                }
            }
        }
        Err(e) => {
            error!("Failed to fetch chunks for deletion of session {}: {}", session_id, e);
            let error_response = ErrorResponse {
                error: "chunks_fetch_failed".to_string(),
                message: e.to_string(),
            };
            (StatusCode::INTERNAL_SERVER_ERROR, Json(error_response)).into_response()
        }
    }
}

/// Internal function to fetch complete transcript
async fn fetch_complete_transcript_internal(
    session_id: Uuid,
    state: &TranscriptApiState,
) -> Result<CompleteTranscriptResponse, TranscriptError> {
    // Get session info
    let session_info = state.db_service.get_session(session_id).await?
        .ok_or_else(|| TranscriptError::SessionNotFound { 
            session_id: session_id.to_string() 
        })?;

    // Get all chunks for the session
    let chunks = state.db_service.get_session_chunks(session_id).await?;
    
    if chunks.is_empty() {
        return Ok(CompleteTranscriptResponse {
            session_id,
            session_info,
            transcript_items: Vec::new(),
            total_items: 0,
            total_chunks: 0,
            total_size_bytes: 0,
        });
    }

    // Download and parse all chunks
    let mut all_items = Vec::new();
    let mut total_size_bytes = 0i64;

    for chunk in &chunks {
        debug!("Downloading chunk {} for session {}", chunk.chunk_number, session_id);
        
        let items = state.s3_service.download_transcript_chunk(&chunk.s3_key).await?;
        all_items.extend(items);
        total_size_bytes += chunk.file_size_bytes;
    }

    // Sort items by timestamp to ensure correct order
    all_items.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));

    Ok(CompleteTranscriptResponse {
        session_id,
        session_info,
        total_items: all_items.len(),
        total_chunks: chunks.len(),
        total_size_bytes,
        transcript_items: all_items,
    })
}

/// Internal function to get user sessions
async fn get_user_sessions_internal(
    user_id: &str,
    limit: i64,
    offset: i64,
    state: &TranscriptApiState,
) -> Result<SessionListResponse, TranscriptError> {
    let sessions = state.db_service.get_user_sessions(user_id, Some(limit), Some(offset)).await?;
    
    let mut session_summaries = Vec::new();
    
    for session in sessions {
        let chunks = state.db_service.get_session_chunks(session.id).await?;
        let summary = create_session_summary(session, chunks);
        session_summaries.push(summary);
    }

    Ok(SessionListResponse {
        total_count: session_summaries.len(),
        sessions: session_summaries,
        limit,
        offset,
    })
}

/// Create session summary from session and chunks data
fn create_session_summary(session: DbTranscriptSession, chunks: Vec<DbTranscriptChunk>) -> SessionSummary {
    let total_items: i32 = chunks.iter().map(|c| c.item_count).sum();
    let total_size_bytes: i64 = chunks.iter().map(|c| c.file_size_bytes).sum();
    
    let duration_minutes = if let Some(ended_at) = session.ended_at {
        let duration = ended_at.signed_duration_since(session.started_at);
        Some(duration.num_minutes() as f64 + (duration.num_seconds() % 60) as f64 / 60.0)
    } else {
        None
    };

    SessionSummary {
        session_id: session.id,
        user_id: session.user_id,
        session_name: session.session_name,
        status: session.status,
        started_at: session.started_at,
        ended_at: session.ended_at,
        total_chunks: chunks.len(),
        total_items,
        total_size_bytes,
        duration_minutes,
    }
}

/// Delete S3 chunk (best effort)
async fn delete_s3_chunk(s3_service: &S3Service, s3_key: &str) -> Result<(), TranscriptError> {
    s3_service.delete_transcript_chunk(s3_key).await
}
