//! Centralized error handling and retry logic for transcript service
//! 
//! This module provides comprehensive error handling, retry strategies,
//! and error reporting for all transcript service operations.

use crate::transcript::{TranscriptError, TranscriptWsMessage};
use axum::{
    extract::ws::{Message, WebSocket},
    http::StatusCode,
    response::{IntoResponse, Json},
};

use serde_json;
use std::time::Duration;
use tokio_retry::{strategy::ExponentialBackoff, Retry};
use tracing::{error, warn, debug};

/// Configuration for retry strategies
#[derive(Clone, Debug)]
pub struct RetryConfig {
    pub max_retries: usize,
    pub initial_delay: Duration,
    pub max_delay: Duration,
    pub backoff_multiplier: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(10),
            backoff_multiplier: 2.0,
        }
    }
}

/// Retry configuration for different operation types
#[derive(Clone, Debug)]
pub struct RetryConfigs {
    pub s3_operations: RetryConfig,
    pub database_operations: RetryConfig,
    pub websocket_operations: RetryConfig,
}

impl Default for RetryConfigs {
    fn default() -> Self {
        Self {
            s3_operations: RetryConfig {
                max_retries: 5,
                initial_delay: Duration::from_millis(200),
                max_delay: Duration::from_secs(30),
                backoff_multiplier: 2.0,
            },
            database_operations: RetryConfig {
                max_retries: 3,
                initial_delay: Duration::from_millis(100),
                max_delay: Duration::from_secs(5),
                backoff_multiplier: 1.5,
            },
            websocket_operations: RetryConfig {
                max_retries: 2,
                initial_delay: Duration::from_millis(50),
                max_delay: Duration::from_secs(2),
                backoff_multiplier: 2.0,
            },
        }
    }
}

/// Error handler for transcript operations
pub struct ErrorHandler {
    configs: RetryConfigs,
}

impl ErrorHandler {
    pub fn new(configs: RetryConfigs) -> Self {
        Self { configs }
    }

    /// Execute S3 operation with retry logic
    pub async fn retry_s3_operation<F, Fut, T, E>(
        &self,
        operation: F,
        operation_name: &str,
    ) -> Result<T, TranscriptError>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<T, E>>,
        E: std::fmt::Display + Send + Sync + 'static,
    {
        let strategy = self.create_retry_strategy(&self.configs.s3_operations);
        
        debug!("Starting S3 operation: {}", operation_name);
        
        match Retry::spawn(strategy, operation).await {
            Ok(result) => {
                debug!("S3 operation succeeded: {}", operation_name);
                Ok(result)
            }
            Err(e) => {
                error!("S3 operation failed after retries: {} - {}", operation_name, e);
                Err(TranscriptError::S3UploadError(format!(
                    "{} failed: {}",
                    operation_name, e
                )))
            }
        }
    }

    /// Execute database operation with retry logic
    pub async fn retry_db_operation<F, Fut, T, E>(
        &self,
        operation: F,
        operation_name: &str,
    ) -> Result<T, TranscriptError>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<T, E>>,
        E: std::fmt::Display + Send + Sync + 'static,
    {
        let strategy = self.create_retry_strategy(&self.configs.database_operations);
        
        debug!("Starting database operation: {}", operation_name);
        
        match Retry::spawn(strategy, operation).await {
            Ok(result) => {
                debug!("Database operation succeeded: {}", operation_name);
                Ok(result)
            }
            Err(e) => {
                error!("Database operation failed after retries: {} - {}", operation_name, e);
                Err(TranscriptError::DatabaseError(format!(
                    "{} failed: {}",
                    operation_name, e
                )))
            }
        }
    }

    /// Execute WebSocket operation with retry logic
    pub async fn retry_websocket_operation<F, Fut, T, E>(
        &self,
        operation: F,
        operation_name: &str,
    ) -> Result<T, TranscriptError>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<T, E>>,
        E: std::fmt::Display + Send + Sync + 'static,
    {
        let strategy = self.create_retry_strategy(&self.configs.websocket_operations);
        
        debug!("Starting WebSocket operation: {}", operation_name);
        
        match Retry::spawn(strategy, operation).await {
            Ok(result) => {
                debug!("WebSocket operation succeeded: {}", operation_name);
                Ok(result)
            }
            Err(e) => {
                error!("WebSocket operation failed after retries: {} - {}", operation_name, e);
                Err(TranscriptError::WebSocketError(format!(
                    "{} failed: {}",
                    operation_name, e
                )))
            }
        }
    }

    /// Send error message over WebSocket
    pub async fn send_websocket_error(
        socket: &mut WebSocket,
        error: &TranscriptError,
        context: &str,
    ) -> Result<(), TranscriptError> {
        let error_message = TranscriptWsMessage::Error {
            message: format!("{}: {}", context, error),
        };

        match serde_json::to_string(&error_message) {
            Ok(json) => {
                if let Err(e) = socket.send(Message::Text(json)).await {
                    error!("Failed to send error message over WebSocket: {}", e);
                    return Err(TranscriptError::WebSocketError(format!(
                        "Failed to send error: {}",
                        e
                    )));
                }
                Ok(())
            }
            Err(e) => {
                error!("Failed to serialize error message: {}", e);
                Err(TranscriptError::SerializationError(format!(
                    "Error serialization failed: {}",
                    e
                )))
            }
        }
    }

    /// Handle and log errors with appropriate severity
    pub fn log_error(&self, error: &TranscriptError, context: &str) {
        match error {
            TranscriptError::SessionNotFound { session_id } => {
                warn!("Session not found in {}: {}", context, session_id);
            }
            TranscriptError::S3UploadError(msg) => {
                error!("S3 error in {}: {}", context, msg);
            }
            TranscriptError::DatabaseError(msg) => {
                error!("Database error in {}: {}", context, msg);
            }
            TranscriptError::SerializationError(msg) => {
                warn!("Serialization error in {}: {}", context, msg);
            }
            TranscriptError::AuthenticationError(msg) => {
                warn!("Authentication error in {}: {}", context, msg);
            }
            TranscriptError::WebSocketError(msg) => {
                error!("WebSocket error in {}: {}", context, msg);
            }
        }
    }

    /// Convert TranscriptError to HTTP response
    pub fn error_to_response(&self, error: &TranscriptError) -> impl IntoResponse {
        let (status_code, error_type, message) = match error {
            TranscriptError::SessionNotFound { session_id } => (
                StatusCode::NOT_FOUND,
                "session_not_found",
                format!("Session {} not found", session_id),
            ),
            TranscriptError::S3UploadError(msg) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "s3_error",
                msg.clone(),
            ),
            TranscriptError::DatabaseError(msg) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "database_error",
                msg.clone(),
            ),
            TranscriptError::SerializationError(msg) => (
                StatusCode::BAD_REQUEST,
                "serialization_error",
                msg.clone(),
            ),
            TranscriptError::AuthenticationError(msg) => (
                StatusCode::UNAUTHORIZED,
                "authentication_error",
                msg.clone(),
            ),
            TranscriptError::WebSocketError(msg) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "websocket_error",
                msg.clone(),
            ),
        };

        let error_response = serde_json::json!({
            "error": error_type,
            "message": message,
            "timestamp": chrono::Utc::now().to_rfc3339(),
        });

        (status_code, Json(error_response))
    }

    /// Create retry strategy from config
    fn create_retry_strategy(&self, config: &RetryConfig) -> impl Iterator<Item = Duration> {
        ExponentialBackoff::from_millis(config.initial_delay.as_millis() as u64)
            .max_delay(config.max_delay)
            .take(config.max_retries)
    }
}

/// Circuit breaker for preventing cascade failures
#[derive(Debug)]
pub struct CircuitBreaker {
    failure_count: std::sync::atomic::AtomicUsize,
    failure_threshold: usize,
    reset_timeout: Duration,
    last_failure: std::sync::Mutex<Option<std::time::Instant>>,
}

impl CircuitBreaker {
    pub fn new(failure_threshold: usize, reset_timeout: Duration) -> Self {
        Self {
            failure_count: std::sync::atomic::AtomicUsize::new(0),
            failure_threshold,
            reset_timeout,
            last_failure: std::sync::Mutex::new(None),
        }
    }

    /// Check if circuit breaker allows operation
    pub fn can_execute(&self) -> bool {
        let current_failures = self.failure_count.load(std::sync::atomic::Ordering::Relaxed);
        
        if current_failures < self.failure_threshold {
            return true;
        }

        // Check if reset timeout has passed
        if let Ok(last_failure) = self.last_failure.lock() {
            if let Some(last_time) = *last_failure {
                if last_time.elapsed() > self.reset_timeout {
                    // Reset circuit breaker
                    self.failure_count.store(0, std::sync::atomic::Ordering::Relaxed);
                    return true;
                }
            }
        }

        false
    }

    /// Record successful operation
    pub fn record_success(&self) {
        self.failure_count.store(0, std::sync::atomic::Ordering::Relaxed);
    }

    /// Record failed operation
    pub fn record_failure(&self) {
        self.failure_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if let Ok(mut last_failure) = self.last_failure.lock() {
            *last_failure = Some(std::time::Instant::now());
        }
    }
}
