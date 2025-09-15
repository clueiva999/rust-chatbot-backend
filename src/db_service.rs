//! Database service for transcript metadata storage
//! 
//! This module handles all database operations for storing transcript session
//! metadata and chunk information using PostgreSQL with sqlx.

use crate::transcript::{TranscriptError, ChunkMetadata, DbTranscriptSession, DbTranscriptChunk};
use chrono::Utc;
use sqlx::PgPool;
use std::time::Duration;
use tokio_retry::{strategy::ExponentialBackoff, Retry};
use tracing::{debug, info};
use uuid::Uuid;

/// Configuration for database service
#[derive(Clone, Debug)]
pub struct DbServiceConfig {
    pub database_url: String,
    pub max_connections: u32,
    pub connection_timeout: Duration,
    pub max_retries: usize,
    pub initial_retry_delay: Duration,
    pub max_retry_delay: Duration,
}

impl Default for DbServiceConfig {
    fn default() -> Self {
        Self {
            database_url: std::env::var("DATABASE_URL")
                .expect("DATABASE_URL environment variable must be set"),
            max_connections: 10,
            connection_timeout: Duration::from_secs(30),
            max_retries: 3,
            initial_retry_delay: Duration::from_millis(100),
            max_retry_delay: Duration::from_secs(5),
        }
    }
}

/// Database service for transcript operations
pub struct DbService {
    pool: PgPool,
    config: DbServiceConfig,
}

impl DbService {
    /// Create a new database service instance
    pub async fn new(config: DbServiceConfig) -> Result<Self, TranscriptError> {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(config.max_connections)
            .acquire_timeout(config.connection_timeout)
            .connect(&config.database_url)
            .await
            .map_err(|e| TranscriptError::DatabaseError(format!("Failed to connect to database: {}", e)))?;

        info!("Connected to PostgreSQL database");

        Ok(Self { pool, config })
    }

    /// Create a new transcript session with a specific session ID
    pub async fn create_session(
        &self,
        session_id: Uuid,
        user_id: &str,
        session_name: Option<&str>,
    ) -> Result<DbTranscriptSession, TranscriptError> {
        let retry_strategy = self.get_retry_strategy();

        let session = Retry::spawn(retry_strategy, || async {
            sqlx::query_as::<_, DbTranscriptSession>(
                r#"
                INSERT INTO transcript_sessions (id, user_id, session_name, status, started_at, created_at, updated_at)
                VALUES ($1, $2, $3, 'active', NOW(), NOW(), NOW())
                RETURNING id, user_id, session_name, status, started_at, ended_at, created_at, updated_at
                "#
            )
            .bind(session_id)
            .bind(user_id)
            .bind(session_name)
            .fetch_one(&self.pool)
            .await
        })
        .await
        .map_err(|e| TranscriptError::DatabaseError(format!("Failed to create session: {}", e)))?;

        info!("Created transcript session: {} for user: {}", session_id, user_id);
        Ok(session)
    }

    /// Update session status
    pub async fn update_session_status(
        &self,
        session_id: Uuid,
        status: &str,
    ) -> Result<(), TranscriptError> {
        let retry_strategy = self.get_retry_strategy();

        Retry::spawn(retry_strategy, || async {
            let ended_at = if status == "completed" || status == "ended" {
                Some(Utc::now())
            } else {
                None
            };

            sqlx::query(
                r#"
                UPDATE transcript_sessions
                SET status = $1, ended_at = $2, updated_at = NOW()
                WHERE id = $3
                "#
            )
            .bind(status)
            .bind(ended_at)
            .bind(session_id)
            .execute(&self.pool)
            .await
        })
        .await
        .map_err(|e| TranscriptError::DatabaseError(format!("Failed to update session status: {}", e)))?;

        debug!("Updated session {} status to: {}", session_id, status);
        Ok(())
    }

    /// Store chunk metadata after successful S3 upload
    pub async fn store_chunk_metadata(
        &self,
        session_id: Uuid,
        metadata: &ChunkMetadata,
    ) -> Result<DbTranscriptChunk, TranscriptError> {
        let retry_strategy = self.get_retry_strategy();

        let chunk = Retry::spawn(retry_strategy, || async {
            sqlx::query_as::<_, DbTranscriptChunk>(
                r#"
                INSERT INTO transcript_chunks (
                    session_id, chunk_number, s3_bucket, s3_key,
                    item_count, file_size_bytes, start_timestamp, end_timestamp, created_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
                RETURNING id, session_id, chunk_number, s3_bucket, s3_key,
                         item_count, file_size_bytes, start_timestamp, end_timestamp, created_at
                "#
            )
            .bind(session_id)
            .bind(metadata.chunk_number as i32)
            .bind(&metadata.s3_bucket)
            .bind(&metadata.s3_key)
            .bind(metadata.item_count as i32)
            .bind(metadata.file_size_bytes as i64)
            .bind(metadata.start_timestamp)
            .bind(metadata.end_timestamp)
            .fetch_one(&self.pool)
            .await
        })
        .await
        .map_err(|e| TranscriptError::DatabaseError(format!("Failed to store chunk metadata: {}", e)))?;

        debug!(
            "Stored chunk metadata: session={}, chunk={}, size={}",
            session_id, metadata.chunk_number, metadata.file_size_bytes
        );

        Ok(chunk)
    }

    /// Get all chunks for a session, ordered by chunk number
    pub async fn get_session_chunks(
        &self,
        session_id: Uuid,
    ) -> Result<Vec<DbTranscriptChunk>, TranscriptError> {
        let retry_strategy = self.get_retry_strategy();

        let chunks = Retry::spawn(retry_strategy, || async {
            sqlx::query_as::<_, DbTranscriptChunk>(
                r#"
                SELECT id, session_id, chunk_number, s3_bucket, s3_key,
                       item_count, file_size_bytes, start_timestamp, end_timestamp, created_at
                FROM transcript_chunks
                WHERE session_id = $1
                ORDER BY chunk_number ASC
                "#
            )
            .bind(session_id)
            .fetch_all(&self.pool)
            .await
        })
        .await
        .map_err(|e| TranscriptError::DatabaseError(format!("Failed to get session chunks: {}", e)))?;

        debug!("Retrieved {} chunks for session {}", chunks.len(), session_id);
        Ok(chunks)
    }

    /// Get session by ID
    pub async fn get_session(&self, session_id: Uuid) -> Result<Option<DbTranscriptSession>, TranscriptError> {
        let retry_strategy = self.get_retry_strategy();

        let session = Retry::spawn(retry_strategy, || async {
            sqlx::query_as::<_, DbTranscriptSession>(
                r#"
                SELECT id, user_id, session_name, status, started_at, ended_at, created_at, updated_at
                FROM transcript_sessions
                WHERE id = $1
                "#
            )
            .bind(session_id)
            .fetch_optional(&self.pool)
            .await
        })
        .await
        .map_err(|e| TranscriptError::DatabaseError(format!("Failed to get session: {}", e)))?;

        Ok(session)
    }

    /// Get sessions for a user
    pub async fn get_user_sessions(
        &self,
        user_id: &str,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> Result<Vec<DbTranscriptSession>, TranscriptError> {
        let retry_strategy = self.get_retry_strategy();
        let limit = limit.unwrap_or(50);
        let offset = offset.unwrap_or(0);

        let sessions = Retry::spawn(retry_strategy, || async {
            sqlx::query_as::<_, DbTranscriptSession>(
                r#"
                SELECT id, user_id, session_name, status, started_at, ended_at, created_at, updated_at
                FROM transcript_sessions
                WHERE user_id = $1
                ORDER BY created_at DESC
                LIMIT $2 OFFSET $3
                "#
            )
            .bind(user_id)
            .bind(limit)
            .bind(offset)
            .fetch_all(&self.pool)
            .await
        })
        .await
        .map_err(|e| TranscriptError::DatabaseError(format!("Failed to get user sessions: {}", e)))?;

        debug!("Retrieved {} sessions for user {}", sessions.len(), user_id);
        Ok(sessions)
    }

    /// Delete a session and all its chunks
    pub async fn delete_session(&self, session_id: Uuid) -> Result<(), TranscriptError> {
        let retry_strategy = self.get_retry_strategy();

        Retry::spawn(retry_strategy, || async {
            // Start a transaction
            let mut tx = self.pool.begin().await?;

            // Delete chunks first (due to foreign key constraint)
            sqlx::query("DELETE FROM transcript_chunks WHERE session_id = $1")
                .bind(session_id)
                .execute(&mut *tx)
                .await?;

            // Delete session
            sqlx::query("DELETE FROM transcript_sessions WHERE id = $1")
                .bind(session_id)
                .execute(&mut *tx)
                .await?;

            // Commit transaction
            tx.commit().await?;

            Ok::<(), sqlx::Error>(())
        })
        .await
        .map_err(|e| TranscriptError::DatabaseError(format!("Failed to delete session: {}", e)))?;

        info!("Deleted session and all chunks: {}", session_id);
        Ok(())
    }

    /// Health check for database connection
    pub async fn health_check(&self) -> Result<(), TranscriptError> {
        sqlx::query("SELECT 1")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| TranscriptError::DatabaseError(format!("Database health check failed: {}", e)))?;

        info!("Database health check passed");
        Ok(())
    }

    /// Get retry strategy for database operations
    fn get_retry_strategy(&self) -> impl Iterator<Item = Duration> {
        ExponentialBackoff::from_millis(self.config.initial_retry_delay.as_millis() as u64)
            .max_delay(self.config.max_retry_delay)
            .take(self.config.max_retries)
    }
}
