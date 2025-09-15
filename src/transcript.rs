//! Transcript streaming service types and data structures
//! 
//! This module contains all the core data structures for the real-time
//! transcript streaming service, including TranscriptItem, TranscriptBuffer,
//! and related types for managing transcript sessions and S3 chunks.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::sync::broadcast;
use uuid::Uuid;

/// JWT authentication result for transcript sessions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthenticatedUser {
    pub user_id: String,
    pub email: Option<String>,
    pub is_pro: bool,
}

/// A single transcript item representing spoken text from a speaker
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TranscriptItem {
    pub speaker_id: String,
    pub text: String,
    pub timestamp: DateTime<Utc>,
}

/// Configuration for transcript buffer behavior
#[derive(Clone, Debug)]
pub struct TranscriptBufferConfig {
    pub buffer_size_limit: usize,
    pub time_limit: Duration,
}

impl Default for TranscriptBufferConfig {
    fn default() -> Self {
        Self {
            buffer_size_limit: 50,
            time_limit: Duration::from_secs(30),
        }
    }
}

/// Buffer for accumulating transcript items before flushing to S3
#[derive(Debug)]
pub struct TranscriptBuffer {
    pub session_id: String,
    pub buffer: Vec<TranscriptItem>,
    pub config: TranscriptBufferConfig,
    pub last_flush: Instant,
    pub chunk_number: u32,
}

impl TranscriptBuffer {
    pub fn new(session_id: String, config: TranscriptBufferConfig) -> Self {
        Self {
            session_id,
            buffer: Vec::new(),
            config,
            last_flush: Instant::now(),
            chunk_number: 0,
        }
    }

    /// Add a transcript item to the buffer
    pub fn add_item(&mut self, item: TranscriptItem) {
        self.buffer.push(item);
    }

    /// Check if buffer should be flushed based on size or time limits
    pub fn should_flush(&self) -> bool {
        self.buffer.len() >= self.config.buffer_size_limit 
            || self.last_flush.elapsed() >= self.config.time_limit
    }

    /// Get items for flushing and prepare for next chunk
    pub fn prepare_flush(&mut self) -> Vec<TranscriptItem> {
        let items = std::mem::take(&mut self.buffer);
        self.last_flush = Instant::now();
        self.chunk_number += 1;
        items
    }

    /// Get the current chunk number
    pub fn current_chunk_number(&self) -> u32 {
        self.chunk_number
    }
}

/// Session management for transcript streaming
#[derive(Debug)]
pub struct TranscriptSession {
    pub session_id: String,
    pub user_id: String,
    pub buffer: TranscriptBuffer,
    pub sender: broadcast::Sender<TranscriptItem>,
    pub created_at: DateTime<Utc>,
}

impl TranscriptSession {
    pub fn new(session_id: String, user_id: String, config: TranscriptBufferConfig) -> Self {
        let (sender, _) = broadcast::channel(1000); // Buffer up to 1000 messages
        
        Self {
            buffer: TranscriptBuffer::new(session_id.clone(), config),
            session_id,
            user_id,
            sender,
            created_at: Utc::now(),
        }
    }

    /// Add item to buffer and broadcast to connected clients
    pub fn add_item(&mut self, item: TranscriptItem) -> Result<(), broadcast::error::SendError<TranscriptItem>> {
        self.buffer.add_item(item.clone());
        self.sender.send(item)?;
        Ok(())
    }

    /// Subscribe to live updates for this session
    pub fn subscribe(&self) -> broadcast::Receiver<TranscriptItem> {
        self.sender.subscribe()
    }
}

/// Database model for transcript sessions
#[derive(Serialize, Deserialize, Debug, Clone, sqlx::FromRow)]
pub struct DbTranscriptSession {
    pub id: Uuid,
    pub user_id: String,
    pub session_name: Option<String>,
    pub status: String,
    pub started_at: DateTime<Utc>,
    pub ended_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Database model for transcript chunks
#[derive(Serialize, Deserialize, Debug, Clone, sqlx::FromRow)]
pub struct DbTranscriptChunk {
    pub id: Uuid,
    pub session_id: Uuid,
    pub chunk_number: i32,
    pub s3_bucket: String,
    pub s3_key: String,
    pub item_count: i32,
    pub file_size_bytes: i64,
    pub start_timestamp: Option<DateTime<Utc>>,
    pub end_timestamp: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

/// S3 chunk metadata for upload operations
#[derive(Debug, Clone)]
pub struct ChunkMetadata {
    pub session_id: String,
    pub chunk_number: u32,
    pub s3_bucket: String,
    pub s3_key: String,
    pub item_count: usize,
    pub file_size_bytes: u64,
    pub start_timestamp: Option<DateTime<Utc>>,
    pub end_timestamp: Option<DateTime<Utc>>,
}

impl ChunkMetadata {
    pub fn from_items(
        session_id: String,
        chunk_number: u32,
        s3_bucket: String,
        s3_key: String,
        items: &[TranscriptItem],
        file_size_bytes: u64,
    ) -> Self {
        let start_timestamp = items.first().map(|item| item.timestamp);
        let end_timestamp = items.last().map(|item| item.timestamp);

        Self {
            session_id,
            chunk_number,
            s3_bucket,
            s3_key,
            item_count: items.len(),
            file_size_bytes,
            start_timestamp,
            end_timestamp,
        }
    }
}

/// WebSocket message types for transcript streaming
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
pub enum TranscriptWsMessage {
    /// Client sends authentication token
    #[serde(rename = "authenticate")]
    Authenticate { token: String },

    /// Client sends transcript item
    #[serde(rename = "transcript_item")]
    TranscriptItem(TranscriptItem),

    /// Server sends authentication response
    #[serde(rename = "auth_response")]
    AuthResponse {
        success: bool,
        error: Option<String>,
        user: Option<AuthenticatedUser>
    },

    /// Server sends transcript item to subscribers
    #[serde(rename = "live_transcript")]
    LiveTranscript(TranscriptItem),

    /// Server sends error message
    #[serde(rename = "error")]
    Error { message: String },

    /// Server sends success confirmation
    #[serde(rename = "success")]
    Success { message: String },
}

/// Error types for transcript operations
#[derive(Debug, thiserror::Error)]
pub enum TranscriptError {
    #[error("Session not found: {session_id}")]
    SessionNotFound { session_id: String },
    
    #[error("S3 upload failed: {0}")]
    S3UploadError(String),
    
    #[error("Database error: {0}")]
    DatabaseError(String),
    
    #[error("Serialization error: {0}")]
    SerializationError(String),
    
    #[error("Authentication error: {0}")]
    AuthenticationError(String),
    
    #[error("WebSocket error: {0}")]
    WebSocketError(String),
}

/// Session manager for handling multiple transcript sessions
pub type SessionManager = HashMap<String, TranscriptSession>;
