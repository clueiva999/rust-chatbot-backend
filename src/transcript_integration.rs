//! Integration module for transcript service components
//! 
//! This module coordinates between the transcript service, S3 service,
//! database service, and error handling to provide a complete solution.

use crate::transcript::{TranscriptItem, TranscriptError, ChunkMetadata};
use crate::transcript_service::TranscriptService;
use crate::s3_service::S3Service;
use crate::db_service::DbService;
use crate::error_handler::{ErrorHandler, RetryConfigs};
use std::sync::Arc;
use tracing::{error, info, warn};
use uuid::Uuid;

/// Integrated transcript service that coordinates all components
pub struct IntegratedTranscriptService {
    transcript_service: Arc<TranscriptService>,
    s3_service: Arc<S3Service>,
    db_service: Arc<DbService>,
    error_handler: ErrorHandler,
}

impl IntegratedTranscriptService {
    pub fn new(
        transcript_service: Arc<TranscriptService>,
        s3_service: Arc<S3Service>,
        db_service: Arc<DbService>,
    ) -> Self {
        let error_handler = ErrorHandler::new(RetryConfigs::default());
        
        Self {
            transcript_service,
            s3_service,
            db_service,
            error_handler,
        }
    }

    /// Start the integrated transcript service with background flush handling
    pub async fn start(self: Arc<Self>) -> Result<(), TranscriptError> {
        info!("Starting integrated transcript service");

        // Perform health checks
        self.health_check().await?;

        // Start the background flush handler
        let service_clone = Arc::clone(&self);
        self.transcript_service
            .clone()
            .start_flush_checker(move |session_id, items, metadata| {
                let service = Arc::clone(&service_clone);
                async move {
                    service.handle_flush(session_id, items, metadata).await
                }
            })
            .await?;

        info!("Integrated transcript service started successfully");
        Ok(())
    }

    /// Handle flushing transcript items to S3 and database
    async fn handle_flush(
        &self,
        session_id: String,
        items: Vec<TranscriptItem>,
        metadata: ChunkMetadata,
    ) -> Result<(), TranscriptError> {
        info!(
            "Handling flush for session {}: {} items, chunk {}",
            session_id, items.len(), metadata.chunk_number
        );

        // Step 1: Upload to S3 with retry logic
        let updated_metadata = self
            .error_handler
            .retry_s3_operation(
                || self.s3_service.upload_transcript_chunk(items.clone(), metadata.clone()),
                &format!("upload_chunk_{}_{}", session_id, metadata.chunk_number),
            )
            .await?;

        // Step 2: Store metadata in database with retry logic
        let session_uuid = Uuid::parse_str(&session_id)
            .map_err(|e| TranscriptError::SerializationError(format!("Invalid session UUID: {}", e)))?;

        self.error_handler
            .retry_db_operation(
                || self.db_service.store_chunk_metadata(session_uuid, &updated_metadata),
                &format!("store_metadata_{}_{}", session_id, metadata.chunk_number),
            )
            .await?;

        info!(
            "Successfully flushed chunk {} for session {}: {} bytes",
            metadata.chunk_number, session_id, updated_metadata.file_size_bytes
        );

        Ok(())
    }

    /// Perform comprehensive health check
    pub async fn health_check(&self) -> Result<(), TranscriptError> {
        info!("Performing transcript service health check");

        // Check S3 connectivity
        if let Err(e) = self.s3_service.health_check().await {
            error!("S3 health check failed: {}", e);
            return Err(e);
        }

        // Check database connectivity
        if let Err(e) = self.db_service.health_check().await {
            error!("Database health check failed: {}", e);
            return Err(e);
        }

        info!("All transcript service health checks passed");
        Ok(())
    }

    /// Create a new transcript session with database record
    pub async fn create_session_with_db(
        &self,
        session_id: String,
        user_id: String,
        session_name: Option<String>,
    ) -> Result<(), TranscriptError> {
        info!("Creating session {} for user {}", session_id, user_id);

        // Create session in transcript service
        self.transcript_service.create_session(session_id.clone(), user_id.clone())?;

        // Parse session ID as UUID for database
        let session_uuid = Uuid::parse_str(&session_id)
            .map_err(|e| TranscriptError::SerializationError(format!("Invalid session UUID: {}", e)))?;

        // Create database record with the same session ID
        let db_session = self
            .error_handler
            .retry_db_operation(
                || self.db_service.create_session(session_uuid, &user_id, session_name.as_deref()),
                &format!("create_session_{}", session_id),
            )
            .await?;

        info!(
            "Created session {} in database with ID {}",
            session_id, db_session.id
        );

        Ok(())
    }

    /// End a session and update its status
    pub async fn end_session(
        &self,
        session_id: String,
    ) -> Result<(), TranscriptError> {
        info!("Ending session: {}", session_id);

        // Parse session ID as UUID for database operations
        let session_uuid = Uuid::parse_str(&session_id)
            .map_err(|e| TranscriptError::SerializationError(format!("Invalid session UUID: {}", e)))?;

        // Update session status in database
        self.error_handler
            .retry_db_operation(
                || self.db_service.update_session_status(session_uuid, "completed"),
                &format!("end_session_{}", session_id),
            )
            .await?;

        // Remove session from transcript service
        self.transcript_service.remove_session(&session_id)?;

        info!("Successfully ended session: {}", session_id);
        Ok(())
    }

    /// Get service statistics for monitoring
    pub fn get_service_stats(&self) -> ServiceStats {
        ServiceStats {
            active_sessions: self.transcript_service.session_count(),
            sessions_needing_flush: self.transcript_service.get_sessions_needing_flush().len(),
        }
    }

    /// Force flush all sessions (for testing or maintenance)
    pub async fn force_flush_all_sessions(&self) -> Result<Vec<String>, TranscriptError> {
        let sessions_to_flush = self.transcript_service.get_sessions_needing_flush();
        let mut flushed_sessions = Vec::new();

        for session_id in sessions_to_flush {
            match self.transcript_service.prepare_session_flush(&session_id) {
                Ok((items, metadata)) => {
                    if let Err(e) = self.handle_flush(session_id.clone(), items, metadata).await {
                        error!("Failed to force flush session {}: {}", session_id, e);
                        self.error_handler.log_error(&e, &format!("force_flush_{}", session_id));
                    } else {
                        flushed_sessions.push(session_id);
                    }
                }
                Err(e) => {
                    error!("Failed to prepare flush for session {}: {}", session_id, e);
                    self.error_handler.log_error(&e, &format!("prepare_flush_{}", session_id));
                }
            }
        }

        info!("Force flushed {} sessions", flushed_sessions.len());
        Ok(flushed_sessions)
    }

    /// Get error handler for external use
    pub fn error_handler(&self) -> &ErrorHandler {
        &self.error_handler
    }

    /// Get transcript service for external use
    pub fn transcript_service(&self) -> &Arc<TranscriptService> {
        &self.transcript_service
    }

    /// Get S3 service for external use
    pub fn s3_service(&self) -> &Arc<S3Service> {
        &self.s3_service
    }

    /// Get database service for external use
    pub fn db_service(&self) -> &Arc<DbService> {
        &self.db_service
    }
}

/// Service statistics for monitoring
#[derive(Debug, Clone)]
pub struct ServiceStats {
    pub active_sessions: usize,
    pub sessions_needing_flush: usize,
}

/// Graceful shutdown handler
pub async fn graceful_shutdown(service: Arc<IntegratedTranscriptService>) -> Result<(), TranscriptError> {
    info!("Starting graceful shutdown of transcript service");

    // Force flush all pending sessions
    match service.force_flush_all_sessions().await {
        Ok(flushed_sessions) => {
            info!("Flushed {} sessions during shutdown", flushed_sessions.len());
        }
        Err(e) => {
            warn!("Some sessions failed to flush during shutdown: {}", e);
        }
    }

    info!("Transcript service shutdown completed");
    Ok(())
}
