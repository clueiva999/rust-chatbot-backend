//! WebSocket handler for real-time transcript streaming
//! 
//! This module implements the WebSocket endpoint for receiving transcript items
//! from Electron apps and broadcasting them to connected Next.js clients.

use crate::transcript::{TranscriptWsMessage, TranscriptError, AuthenticatedUser};
use crate::transcript_service::TranscriptService;
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, State,
    },
    http::StatusCode,
    response::IntoResponse,
};
use futures_util::{SinkExt, StreamExt};
use serde_json;
use std::sync::Arc;
use uuid::Uuid;

use tracing::{debug, error, info, warn};

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
    State(app_state): State<Arc<crate::AppState>>,
) -> impl IntoResponse {
    info!("WebSocket upgrade request for session: {}", session_id);

    ws.on_upgrade(move |socket| {
        handle_transcript_websocket(socket, session_id, app_state)
    })
}

/// Main WebSocket handler for transcript streaming
async fn handle_transcript_websocket(
    socket: WebSocket,
    session_id: String,
    app_state: Arc<crate::AppState>,
) {
    let connection_id = Uuid::new_v4().to_string();
    info!("New transcript WebSocket connection: {} for session: {} (authentication required)", connection_id, session_id);

    let state = Arc::new(tokio::sync::RwLock::new(ConnectionState {
        session_id: session_id.clone(),
        user: None,
        is_authenticated: false,
    }));

    // Split the WebSocket into sender and receiver
    let (ws_sender, mut ws_receiver) = socket.split();
    let ws_sender = Arc::new(tokio::sync::Mutex::new(ws_sender));

    // We'll create the live receiver only after authentication
    let live_receiver: Arc<tokio::sync::Mutex<Option<tokio::sync::broadcast::Receiver<crate::transcript::TranscriptItem>>>> = Arc::new(tokio::sync::Mutex::new(None));

    // Spawn task to handle incoming messages from client
    let transcript_service_clone = Arc::clone(&app_state.transcript_service);
    let integrated_service_clone = Arc::clone(&app_state.integrated_service);
    let session_id_clone = session_id.clone();
    let state_clone = Arc::clone(&state);
    let ws_sender_clone = Arc::clone(&ws_sender);
    let live_receiver_clone = Arc::clone(&live_receiver);
    let incoming_task = tokio::spawn(async move {
        while let Some(msg) = ws_receiver.next().await {
            match msg {
                Ok(Message::Text(text)) => {
                    if let Err(e) = handle_incoming_message(
                        &text,
                        &session_id_clone,
                        &transcript_service_clone,
                        Arc::clone(&integrated_service_clone),
                        Arc::clone(&state_clone),
                        Arc::clone(&ws_sender_clone),
                        Arc::clone(&live_receiver_clone),
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
    let ws_sender_clone = Arc::clone(&ws_sender);
    let live_receiver_clone = Arc::clone(&live_receiver);
    let outgoing_task = tokio::spawn(async move {
        loop {
            // Check if we have a live receiver (only after authentication)
            let receiver_opt = {
                let mut receiver_guard = live_receiver_clone.lock().await;
                receiver_guard.take()
            };

            if let Some(mut receiver) = receiver_opt {
                // We have a receiver, listen for transcript items
                match receiver.recv().await {
                    Ok(transcript_item) => {
                        let message = TranscriptWsMessage::LiveTranscript(transcript_item);

                        match serde_json::to_string(&message) {
                            Ok(json) => {
                                let mut sender = ws_sender_clone.lock().await;
                                if let Err(e) = sender.send(Message::Text(json)).await {
                                    error!("Failed to send live transcript for session {}: {}", session_id_clone, e);
                                    break;
                                }
                            }
                            Err(e) => {
                                error!("Failed to serialize live transcript for session {}: {}", session_id_clone, e);
                            }
                        }

                        // Put the receiver back
                        *live_receiver_clone.lock().await = Some(receiver);
                    }
                    Err(_) => {
                        // Channel closed, break the loop
                        break;
                    }
                }
            } else {
                // No receiver yet, wait a bit and check again
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
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
    integrated_service: Arc<crate::transcript_integration::IntegratedTranscriptService>,
    state: Arc<tokio::sync::RwLock<ConnectionState>>,
    ws_sender: Arc<tokio::sync::Mutex<futures_util::stream::SplitSink<WebSocket, Message>>>,
    live_receiver: Arc<tokio::sync::Mutex<Option<tokio::sync::broadcast::Receiver<crate::transcript::TranscriptItem>>>>,
) -> Result<(), TranscriptError> {
    let message: TranscriptWsMessage = serde_json::from_str(text)
        .map_err(|e| TranscriptError::SerializationError(format!("Invalid message format: {}", e)))?;

    match message {
        TranscriptWsMessage::Authenticate { token } => {
            debug!("Received authentication request for session: {}", session_id);

            match authenticate_user(&token).await {
                Ok(user) => {
                    // Update connection state
                    {
                        let mut state_guard = state.write().await;
                        state_guard.user = Some(user.clone());
                        state_guard.is_authenticated = true;
                    }

                    info!("Authenticated user {} for session {}", user.user_id, session_id);

                    // Create session in memory and database
                    if let Err(e) = transcript_service.create_session(session_id.to_string(), user.user_id.clone()) {
                        error!("Failed to create session in memory {}: {}", session_id, e);

                        // Send error response
                        let error_response = TranscriptWsMessage::AuthResponse {
                            success: false,
                            error: Some(format!("Failed to create session: {}", e)),
                            user: None,
                        };
                        send_message(ws_sender.clone(), &error_response).await?;
                        return Ok(());
                    }

                    // Create session record in database (async, but don't block WebSocket)
                    let integrated_service_clone = integrated_service.clone();
                    let session_id_clone = session_id.to_string();
                    let user_id_clone = user.user_id.clone();
                    tokio::spawn(async move {
                        if let Err(e) = integrated_service_clone.create_session_with_db(
                            session_id_clone.clone(),
                            user_id_clone,
                            None
                        ).await {
                            error!("Failed to create session in database {}: {}", session_id_clone, e);
                        }
                    });

                    // Subscribe to live transcript updates
                    match transcript_service.subscribe_to_session(session_id) {
                        Ok(receiver) => {
                            *live_receiver.lock().await = Some(receiver);
                        }
                        Err(e) => {
                            warn!("Failed to subscribe to session {}: {}", session_id, e);
                        }
                    }

                    // Send success response
                    let auth_response = TranscriptWsMessage::AuthResponse {
                        success: true,
                        error: None,
                        user: Some(user),
                    };
                    send_message(ws_sender.clone(), &auth_response).await?;
                }
                Err(e) => {
                    error!("Authentication failed for session {}: {}", session_id, e);

                    // Send error response
                    let error_response = TranscriptWsMessage::AuthResponse {
                        success: false,
                        error: Some(e),
                        user: None,
                    };
                    send_message(ws_sender.clone(), &error_response).await?;
                }
            }
        }
        TranscriptWsMessage::TranscriptItem(item) => {
            // Check if authenticated
            let is_authenticated = {
                let state_guard = state.read().await;
                state_guard.is_authenticated
            };

            if !is_authenticated {
                warn!("Received transcript item from unauthenticated connection for session: {}", session_id);
                let error_response = TranscriptWsMessage::Error {
                    message: "Authentication required before sending transcript items".to_string(),
                };
                send_message(ws_sender.clone(), &error_response).await?;
                return Ok(());
            }

            debug!("Received transcript item for session {}: {} chars", session_id, item.text.len());
            transcript_service.add_transcript_item(session_id, item)?;
        }
        _ => {
            warn!("Unexpected message type received for session: {}", session_id);
        }
    }

    Ok(())
}

/// Helper function to send a message over WebSocket
async fn send_message(
    ws_sender: Arc<tokio::sync::Mutex<futures_util::stream::SplitSink<WebSocket, Message>>>,
    message: &TranscriptWsMessage,
) -> Result<(), TranscriptError> {
    match serde_json::to_string(message) {
        Ok(json) => {
            let mut sender = ws_sender.lock().await;
            sender.send(Message::Text(json)).await
                .map_err(|e| TranscriptError::WebSocketError(format!("Failed to send message: {}", e)))?;
            Ok(())
        }
        Err(e) => {
            Err(TranscriptError::SerializationError(format!("Failed to serialize message: {}", e)))
        }
    }
}

/// Authenticate JWT token using the same logic as the main /ws endpoint
async fn authenticate_user(token: &str) -> Result<crate::transcript::AuthenticatedUser, String> {
    let info = fetch_user_context(token).await.map_err(|e| e.to_string())?;

    let u = info.get("user").ok_or("bad user")?;
    let user = crate::transcript::AuthenticatedUser {
        user_id: u.get("id").and_then(|v| v.as_str()).unwrap_or_default().into(),
        email: u.get("email").and_then(|v| v.as_str()).map(|s| s.into()),
        is_pro: u.get("isPro").and_then(|v| v.as_bool()).unwrap_or(false),
    };

    Ok(user)
}

/// Fetch user context from Next.js API (same as main.rs)
async fn fetch_user_context(tok: &str) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
    let cli = reqwest::Client::builder().user_agent("hiremage/1.0").build()?;
    let url = std::env::var("NEXTJS_URL").unwrap_or_else(|_| "https://clueiva.com".into());
    let r = cli.get(format!("{url}/api/user/context")).bearer_auth(tok).send().await?;
    if !r.status().is_success() {
        return Err(format!("ctx http {}", r.status()).into())
    }
    Ok(r.json().await?)
}

/// Health check endpoint for transcript WebSocket service
pub async fn transcript_health_check() -> impl IntoResponse {
    (StatusCode::OK, "Transcript WebSocket service is healthy")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_authenticate_message_parsing() {
        let auth_message = json!({
            "type": "authenticate",
            "token": "test_jwt_token_here"
        });

        let parsed: Result<TranscriptWsMessage, _> = serde_json::from_value(auth_message);
        assert!(parsed.is_ok());

        let message = parsed.unwrap();
        match message {
            TranscriptWsMessage::Authenticate { token } => {
                assert_eq!(token, "test_jwt_token_here");
            }
            _ => panic!("Expected Authenticate message"),
        }
    }

    #[test]
    fn test_auth_response_message_parsing() {
        let auth_response = json!({
            "type": "auth_response",
            "success": true,
            "error": null,
            "user": {
                "user_id": "test_user_123",
                "email": "test@example.com",
                "is_pro": true
            }
        });

        let parsed: Result<TranscriptWsMessage, _> = serde_json::from_value(auth_response);
        assert!(parsed.is_ok());

        let message = parsed.unwrap();
        match message {
            TranscriptWsMessage::AuthResponse { success, error, user } => {
                assert!(success);
                assert!(error.is_none());
                assert!(user.is_some());
                let user = user.unwrap();
                assert_eq!(user.user_id, "test_user_123");
                assert_eq!(user.email, Some("test@example.com".to_string()));
                assert!(user.is_pro);
            }
            _ => panic!("Expected AuthResponse message"),
        }
    }

    #[test]
    fn test_error_auth_response_parsing() {
        let error_response = json!({
            "type": "auth_response",
            "success": false,
            "error": "Invalid token",
            "user": null
        });

        let parsed: Result<TranscriptWsMessage, _> = serde_json::from_value(error_response);
        assert!(parsed.is_ok());

        let message = parsed.unwrap();
        match message {
            TranscriptWsMessage::AuthResponse { success, error, user } => {
                assert!(!success);
                assert_eq!(error, Some("Invalid token".to_string()));
                assert!(user.is_none());
            }
            _ => panic!("Expected AuthResponse message"),
        }
    }
}
