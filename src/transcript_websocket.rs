//! WebSocket handler for real-time transcript streaming
//! 
//! This module implements the WebSocket endpoint for receiving transcript items
//! from Electron apps and broadcasting them to connected Next.js clients.

use crate::transcript::{TranscriptWsMessage, TranscriptError};
use crate::transcript_service::TranscriptService;
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, State,
    },
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
use futures_util::{SinkExt, StreamExt};
use serde_json;
use std::sync::Arc;
use uuid::Uuid;

use tracing::{debug, error, info, warn};

/// JWT authentication result
#[derive(Debug, Clone)]
pub struct AuthenticatedUser {
    pub user_id: String,
    pub email: Option<String>,
    pub is_pro: bool,
}

/// WebSocket connection state
#[derive(Debug)]
struct ConnectionState {
    session_id: String,
    user: Option<AuthenticatedUser>,
    is_authenticated: bool,
}

/// Handle WebSocket upgrade for transcript streaming
pub async fn transcript_websocket_handler(
    ws: WebSocketUpgrade,
    Path(session_id): Path<String>,
    headers: HeaderMap,
    State(app_state): State<Arc<crate::AppState>>,
) -> impl IntoResponse {
    // Extract JWT token from Sec-WebSocket-Protocol header
    let auth_token = headers
        .get("sec-websocket-protocol")
        .and_then(|value| value.to_str().ok())
        .map(|s| s.to_string());

    info!("WebSocket upgrade request for session: {}", session_id);

    ws.on_upgrade(move |socket| {
        handle_transcript_websocket(socket, session_id, auth_token, app_state)
    })
}

/// Main WebSocket handler for transcript streaming
async fn handle_transcript_websocket(
    socket: WebSocket,
    session_id: String,
    auth_token: Option<String>,
    app_state: Arc<crate::AppState>,
) {
    let connection_id = Uuid::new_v4().to_string();
    info!("New transcript WebSocket connection: {} for session: {}", connection_id, session_id);

    let mut state = ConnectionState {
        session_id: session_id.clone(),
        user: None,
        is_authenticated: false,
    };

    // Authenticate the connection
    if let Some(token) = auth_token {
        match authenticate_jwt_token(&token).await {
            Ok(user) => {
                state.user = Some(user.clone());
                state.is_authenticated = true;
                info!("Authenticated user {} for session {}", user.user_id, session_id);

                // Create session in both memory and database
                if let Err(e) = app_state.transcript_service.create_session(session_id.clone(), user.user_id.clone()) {
                    error!("Failed to create session in memory {}: {}", session_id, e);
                    return;
                }

                // Create session record in database (async, but don't block WebSocket)
                let integrated_service = Arc::clone(&app_state.integrated_service);
                let session_id_clone = session_id.clone();
                let user_id_clone = user.user_id.clone();
                tokio::spawn(async move {
                    if let Err(e) = integrated_service.create_session_with_db(
                        session_id_clone.clone(),
                        user_id_clone,
                        None
                    ).await {
                        error!("Failed to create session in database {}: {}", session_id_clone, e);
                        // Continue anyway - session exists in memory for real-time streaming
                    }
                });
            }
            Err(e) => {
                error!("Authentication failed for session {}: {}", session_id, e);
                return;
            }
        }
    } else {
        error!("No authentication token provided for session {}", session_id);
        return;
    }

    // Split the WebSocket into sender and receiver
    let (mut ws_sender, mut ws_receiver) = socket.split();

    // Subscribe to live transcript updates for this session with retry
    let mut live_receiver = {
        let mut attempts = 0;
        loop {
            match app_state.transcript_service.subscribe_to_session(&session_id) {
                Ok(receiver) => break receiver,
                Err(e) => {
                    attempts += 1;
                    if attempts >= 3 {
                        error!("Failed to subscribe to session {} after {} attempts: {}", session_id, attempts, e);
                        return;
                    }
                    warn!("Failed to subscribe to session {} (attempt {}): {}, retrying...", session_id, attempts, e);
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                }
            }
        }
    };

    // Spawn task to handle incoming messages from client
    let transcript_service_clone = Arc::clone(&app_state.transcript_service);
    let session_id_clone = session_id.clone();
    let incoming_task = tokio::spawn(async move {
        while let Some(msg) = ws_receiver.next().await {
            match msg {
                Ok(Message::Text(text)) => {
                    if let Err(e) = handle_incoming_message(
                        &text,
                        &session_id_clone,
                        &transcript_service_clone,
                    ).await {
                        error!("Error handling incoming message for session {}: {}", session_id_clone, e);
                    }
                }
                Ok(Message::Close(_)) => {
                    info!("WebSocket closed for session: {}", session_id_clone);
                    break;
                }
                Err(e) => {
                    error!("WebSocket error for session {}: {}", session_id_clone, e);
                    break;
                }
                _ => {
                    // Ignore other message types (binary, ping, pong)
                }
            }
        }
    });

    // Handle outgoing messages (live transcript broadcasts)
    let session_id_clone = session_id.clone();
    let outgoing_task = tokio::spawn(async move {
        while let Ok(transcript_item) = live_receiver.recv().await {
            let message = TranscriptWsMessage::LiveTranscript(transcript_item);
            
            match serde_json::to_string(&message) {
                Ok(json) => {
                    if let Err(e) = ws_sender.send(Message::Text(json)).await {
                        error!("Failed to send live transcript for session {}: {}", session_id_clone, e);
                        break;
                    }
                }
                Err(e) => {
                    error!("Failed to serialize live transcript for session {}: {}", session_id_clone, e);
                }
            }
        }
    });

    // Wait for either task to complete
    tokio::select! {
        _ = incoming_task => {
            debug!("Incoming message handler completed for session: {}", session_id);
        }
        _ = outgoing_task => {
            debug!("Outgoing message handler completed for session: {}", session_id);
        }
    }

    info!("WebSocket connection closed for session: {}", session_id);
}

/// Handle incoming WebSocket messages
async fn handle_incoming_message(
    text: &str,
    session_id: &str,
    transcript_service: &TranscriptService,
) -> Result<(), TranscriptError> {
    let message: TranscriptWsMessage = serde_json::from_str(text)
        .map_err(|e| TranscriptError::SerializationError(format!("Invalid message format: {}", e)))?;

    match message {
        TranscriptWsMessage::TranscriptItem(item) => {
            debug!("Received transcript item for session {}: {} chars", session_id, item.text.len());
            transcript_service.add_transcript_item(session_id, item)?;
        }
        _ => {
            warn!("Unexpected message type received for session: {}", session_id);
        }
    }

    Ok(())
}

/// Authenticate JWT token (placeholder implementation)
/// In a real implementation, this would validate the JWT against your auth service
async fn authenticate_jwt_token(token: &str) -> Result<AuthenticatedUser, TranscriptError> {
    // TODO: Implement actual JWT validation
    // This is a placeholder that should be replaced with real JWT validation
    // against your Next.js auth service
    
    if token.is_empty() {
        return Err(TranscriptError::AuthenticationError("Empty token".to_string()));
    }

    // For now, we'll do a simple validation
    // In production, you would:
    // 1. Decode and verify the JWT signature
    // 2. Check expiration
    // 3. Validate against your user database
    // 4. Check if user has pro access for transcript features
    
    if token.starts_with("valid_") {
        // Extract user_id from token (this is just for demo)
        let user_id = token.strip_prefix("valid_").unwrap_or("unknown").to_string();
        
        Ok(AuthenticatedUser {
            user_id,
            email: None,
            is_pro: true, // For demo purposes
        })
    } else {
        Err(TranscriptError::AuthenticationError("Invalid token".to_string()))
    }
}

/// Health check endpoint for transcript WebSocket service
pub async fn transcript_health_check() -> impl IntoResponse {
    (StatusCode::OK, "Transcript WebSocket service is healthy")
}
