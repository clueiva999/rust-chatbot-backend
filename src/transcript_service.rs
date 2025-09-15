//! Transcript service for managing sessions and buffer flushing
//! 
//! This module provides the core service for managing transcript sessions,
//! handling buffer flushing, and coordinating between WebSocket connections,
//! S3 uploads, and database operations.

use crate::transcript::{
    TranscriptItem, TranscriptSession, TranscriptBufferConfig,
    TranscriptError, ChunkMetadata
};
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::time::{interval, Interval};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// Configuration for the transcript service
#[derive(Clone, Debug)]
pub struct TranscriptServiceConfig {
    pub buffer_config: TranscriptBufferConfig,
    pub flush_check_interval: Duration,
    pub max_concurrent_uploads: usize,
}

impl Default for TranscriptServiceConfig {
    fn default() -> Self {
        Self {
            buffer_config: TranscriptBufferConfig::default(),
            flush_check_interval: Duration::from_secs(5),
            max_concurrent_uploads: 10,
        }
    }
}

/// Main transcript service managing all sessions
pub struct TranscriptService {
    sessions: Arc<DashMap<String, TranscriptSession>>,
    config: TranscriptServiceConfig,
    flush_interval: Interval,
}

impl TranscriptService {
    pub fn new(config: TranscriptServiceConfig) -> Self {
        let flush_interval = interval(config.flush_check_interval);
        
        Self {
            sessions: Arc::new(DashMap::new()),
            config,
            flush_interval,
        }
    }

    /// Create a new transcript session
    pub fn create_session(&self, session_id: String, user_id: String) -> Result<(), TranscriptError> {
        if self.sessions.contains_key(&session_id) {
            warn!("Session {} already exists", session_id);
            return Ok(()); // Session already exists, not an error
        }

        let session = TranscriptSession::new(
            session_id.clone(),
            user_id,
            self.config.buffer_config.clone(),
        );

        self.sessions.insert(session_id.clone(), session);
        info!("Created transcript session: {}", session_id);
        
        Ok(())
    }

    /// Remove a transcript session
    pub fn remove_session(&self, session_id: &str) -> Result<(), TranscriptError> {
        match self.sessions.remove(session_id) {
            Some(_) => {
                info!("Removed transcript session: {}", session_id);
                Ok(())
            }
            None => {
                warn!("Attempted to remove non-existent session: {}", session_id);
                Err(TranscriptError::SessionNotFound {
                    session_id: session_id.to_string(),
                })
            }
        }
    }

    /// Add a transcript item to a session
    pub fn add_transcript_item(
        &self,
        session_id: &str,
        item: TranscriptItem,
    ) -> Result<(), TranscriptError> {
        match self.sessions.get_mut(session_id) {
            Some(mut session) => {
                session.add_item(item).map_err(|e| {
                    TranscriptError::WebSocketError(format!("Failed to broadcast item: {}", e))
                })?;
                
                debug!("Added transcript item to session {}: {} chars", 
                       session_id, session.buffer.buffer.last().unwrap().text.len());
                
                Ok(())
            }
            None => Err(TranscriptError::SessionNotFound {
                session_id: session_id.to_string(),
            }),
        }
    }

    /// Subscribe to live updates for a session
    pub fn subscribe_to_session(
        &self,
        session_id: &str,
    ) -> Result<broadcast::Receiver<TranscriptItem>, TranscriptError> {
        match self.sessions.get(session_id) {
            Some(session) => Ok(session.subscribe()),
            None => Err(TranscriptError::SessionNotFound {
                session_id: session_id.to_string(),
            }),
        }
    }

    /// Check if a session exists
    pub fn session_exists(&self, session_id: &str) -> bool {
        self.sessions.contains_key(session_id)
    }

    /// Get session count for monitoring
    pub fn session_count(&self) -> usize {
        self.sessions.len()
    }

    /// Get sessions that need flushing
    pub fn get_sessions_needing_flush(&self) -> Vec<String> {
        self.sessions
            .iter()
            .filter_map(|entry| {
                let session_id = entry.key();
                let session = entry.value();
                
                if session.buffer.should_flush() && !session.buffer.buffer.is_empty() {
                    Some(session_id.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    /// Prepare flush data for a session
    pub fn prepare_session_flush(
        &self,
        session_id: &str,
    ) -> Result<(Vec<TranscriptItem>, ChunkMetadata), TranscriptError> {
        match self.sessions.get_mut(session_id) {
            Some(mut session) => {
                let items = session.buffer.prepare_flush();
                
                if items.is_empty() {
                    return Err(TranscriptError::SerializationError(
                        "No items to flush".to_string(),
                    ));
                }

                let chunk_number = session.buffer.current_chunk_number();
                let s3_key = format!(
                    "transcripts/{}/chunks/{}-{}.jsonl",
                    session_id,
                    Utc::now().format("%Y%m%d_%H%M%S"),
                    Uuid::new_v4()
                );

                // Calculate estimated file size (will be updated after compression)
                let estimated_size = items
                    .iter()
                    .map(|item| item.text.len() + 100) // rough estimate including JSON overhead
                    .sum::<usize>() as u64;

                let metadata = ChunkMetadata::from_items(
                    session_id.to_string(),
                    chunk_number,
                    "transcript-chunks".to_string(), // This should come from config
                    s3_key,
                    &items,
                    estimated_size,
                );

                info!(
                    "Prepared flush for session {}: {} items, chunk {}",
                    session_id, items.len(), chunk_number
                );

                Ok((items, metadata))
            }
            None => Err(TranscriptError::SessionNotFound {
                session_id: session_id.to_string(),
            }),
        }
    }

    /// Get session info for debugging
    pub fn get_session_info(&self, session_id: &str) -> Option<SessionInfo> {
        self.sessions.get(session_id).map(|session| SessionInfo {
            session_id: session.session_id.clone(),
            user_id: session.user_id.clone(),
            buffer_size: session.buffer.buffer.len(),
            chunk_number: session.buffer.chunk_number,
            created_at: session.created_at,
            last_flush: session.buffer.last_flush,
        })
    }

    /// Start the background flush checker task
    pub async fn start_flush_checker<F, Fut>(
        self: Arc<Self>,
        flush_handler: F,
    ) -> Result<(), TranscriptError>
    where
        F: Fn(String, Vec<TranscriptItem>, ChunkMetadata) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<(), TranscriptError>> + Send + 'static,
    {
        let service = Arc::clone(&self);
        let flush_handler = Arc::new(flush_handler);
        
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(service.config.flush_check_interval);
            
            loop {
                interval.tick().await;
                
                let sessions_to_flush = service.get_sessions_needing_flush();
                
                if !sessions_to_flush.is_empty() {
                    debug!("Found {} sessions needing flush", sessions_to_flush.len());
                    
                    for session_id in sessions_to_flush {
                        let service_clone = Arc::clone(&service);
                        let handler_clone = Arc::clone(&flush_handler);
                        let session_id_clone = session_id.clone();
                        
                        tokio::spawn(async move {
                            match service_clone.prepare_session_flush(&session_id_clone) {
                                Ok((items, metadata)) => {
                                    if let Err(e) = handler_clone(session_id_clone.clone(), items, metadata).await {
                                        error!("Failed to flush session {}: {}", session_id_clone, e);
                                    }
                                }
                                Err(e) => {
                                    error!("Failed to prepare flush for session {}: {}", session_id_clone, e);
                                }
                            }
                        });
                    }
                }
            }
        });

        info!("Started transcript service flush checker");
        Ok(())
    }
}

/// Session information for debugging and monitoring
#[derive(Debug, Clone)]
pub struct SessionInfo {
    pub session_id: String,
    pub user_id: String,
    pub buffer_size: usize,
    pub chunk_number: u32,
    pub created_at: DateTime<Utc>,
    pub last_flush: std::time::Instant,
}
