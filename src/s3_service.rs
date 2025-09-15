//! S3 service for uploading transcript chunks
//! 
//! This module handles uploading compressed JSONL transcript chunks to Amazon S3
//! with retry logic and proper error handling.

use crate::transcript::{TranscriptItem, TranscriptError, ChunkMetadata};
use aws_config::BehaviorVersion;
use aws_sdk_s3::{Client as S3Client, primitives::ByteStream};
use flate2::{write::GzEncoder, Compression};
use serde_json;
use std::io::Write;
use std::time::Duration;
use tokio_retry::{strategy::ExponentialBackoff, Retry};
use tracing::{debug, error, info};

/// Configuration for S3 upload service
#[derive(Clone, Debug)]
pub struct S3ServiceConfig {
    pub bucket_name: String,
    pub region: String,
    pub max_retries: usize,
    pub initial_retry_delay: Duration,
    pub max_retry_delay: Duration,
    pub compression_level: u32,
}

impl Default for S3ServiceConfig {
    fn default() -> Self {
        Self {
            bucket_name: std::env::var("S3_BUCKET_NAME")
                .unwrap_or_else(|_| "transcript-chunks".to_string()),
            region: std::env::var("AWS_REGION")
                .unwrap_or_else(|_| "us-east-1".to_string()),
            max_retries: 3,
            initial_retry_delay: Duration::from_millis(100),
            max_retry_delay: Duration::from_secs(10),
            compression_level: 6, // Good balance between speed and compression
        }
    }
}

/// S3 upload service for transcript chunks
pub struct S3Service {
    client: S3Client,
    config: S3ServiceConfig,
}

impl S3Service {
    /// Create a new S3 service instance
    pub async fn new(config: S3ServiceConfig) -> Result<Self, TranscriptError> {
        let aws_config = aws_config::defaults(BehaviorVersion::latest())
            .region(aws_config::Region::new(config.region.clone()))
            .load()
            .await;

        let client = S3Client::new(&aws_config);

        info!("Initialized S3 service for bucket: {}", config.bucket_name);

        Ok(Self { client, config })
    }

    /// Upload transcript items as compressed JSONL to S3
    pub async fn upload_transcript_chunk(
        &self,
        items: Vec<TranscriptItem>,
        mut metadata: ChunkMetadata,
    ) -> Result<ChunkMetadata, TranscriptError> {
        let jsonl_data = self.serialize_to_jsonl(&items)?;
        let compressed_data = self.compress_data(&jsonl_data)?;
        
        // Update metadata with actual file size
        metadata.file_size_bytes = compressed_data.len() as u64;

        debug!(
            "Uploading chunk {} for session {}: {} items, {} bytes compressed",
            metadata.chunk_number,
            metadata.session_id,
            metadata.item_count,
            metadata.file_size_bytes
        );

        let retry_strategy = ExponentialBackoff::from_millis(
            self.config.initial_retry_delay.as_millis() as u64
        )
        .max_delay(self.config.max_retry_delay)
        .take(self.config.max_retries);

        let upload_result = Retry::spawn(retry_strategy, || {
            self.upload_to_s3(&metadata.s3_key, compressed_data.clone())
        })
        .await;

        match upload_result {
            Ok(_) => {
                info!(
                    "Successfully uploaded chunk {} for session {} to S3: {}",
                    metadata.chunk_number, metadata.session_id, metadata.s3_key
                );
                Ok(metadata)
            }
            Err(e) => {
                error!(
                    "Failed to upload chunk {} for session {} after {} retries: {}",
                    metadata.chunk_number, metadata.session_id, self.config.max_retries, e
                );
                Err(TranscriptError::S3UploadError(format!(
                    "Upload failed after retries: {}",
                    e
                )))
            }
        }
    }

    /// Download and parse a transcript chunk from S3
    pub async fn download_transcript_chunk(
        &self,
        s3_key: &str,
    ) -> Result<Vec<TranscriptItem>, TranscriptError> {
        debug!("Downloading transcript chunk from S3: {}", s3_key);

        let retry_strategy = ExponentialBackoff::from_millis(
            self.config.initial_retry_delay.as_millis() as u64
        )
        .max_delay(self.config.max_retry_delay)
        .take(self.config.max_retries);

        let download_result = Retry::spawn(retry_strategy, || {
            self.download_from_s3(s3_key)
        })
        .await;

        match download_result {
            Ok(compressed_data) => {
                let jsonl_data = self.decompress_data(&compressed_data)?;
                let items = self.parse_jsonl(&jsonl_data)?;
                
                debug!(
                    "Successfully downloaded and parsed {} items from S3: {}",
                    items.len(),
                    s3_key
                );
                
                Ok(items)
            }
            Err(e) => {
                error!(
                    "Failed to download chunk from S3 after {} retries: {} - {}",
                    self.config.max_retries, s3_key, e
                );
                Err(TranscriptError::S3UploadError(format!(
                    "Download failed after retries: {}",
                    e
                )))
            }
        }
    }

    /// Serialize transcript items to JSONL format
    fn serialize_to_jsonl(&self, items: &[TranscriptItem]) -> Result<String, TranscriptError> {
        let mut jsonl = String::new();
        
        for item in items {
            let json_line = serde_json::to_string(item)
                .map_err(|e| TranscriptError::SerializationError(e.to_string()))?;
            jsonl.push_str(&json_line);
            jsonl.push('\n');
        }

        Ok(jsonl)
    }

    /// Parse JSONL data back to transcript items
    fn parse_jsonl(&self, jsonl_data: &str) -> Result<Vec<TranscriptItem>, TranscriptError> {
        let mut items = Vec::new();
        
        for line in jsonl_data.lines() {
            if line.trim().is_empty() {
                continue;
            }
            
            let item: TranscriptItem = serde_json::from_str(line)
                .map_err(|e| TranscriptError::SerializationError(e.to_string()))?;
            items.push(item);
        }

        Ok(items)
    }

    /// Compress data using gzip
    fn compress_data(&self, data: &str) -> Result<Vec<u8>, TranscriptError> {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::new(self.config.compression_level));
        encoder
            .write_all(data.as_bytes())
            .map_err(|e| TranscriptError::SerializationError(format!("Compression failed: {}", e)))?;
        
        encoder
            .finish()
            .map_err(|e| TranscriptError::SerializationError(format!("Compression finish failed: {}", e)))
    }

    /// Decompress gzip data
    fn decompress_data(&self, compressed_data: &[u8]) -> Result<String, TranscriptError> {
        use flate2::read::GzDecoder;
        use std::io::Read;

        let mut decoder = GzDecoder::new(compressed_data);
        let mut decompressed = String::new();
        
        decoder
            .read_to_string(&mut decompressed)
            .map_err(|e| TranscriptError::SerializationError(format!("Decompression failed: {}", e)))?;

        Ok(decompressed)
    }

    /// Upload data to S3
    async fn upload_to_s3(&self, s3_key: &str, data: Vec<u8>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let body = ByteStream::from(data);

        self.client
            .put_object()
            .bucket(&self.config.bucket_name)
            .key(s3_key)
            .body(body)
            .content_type("application/x-jsonlines")
            .content_encoding("gzip")
            .send()
            .await?;

        Ok(())
    }

    /// Download data from S3
    async fn download_from_s3(&self, s3_key: &str) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
        let response = self.client
            .get_object()
            .bucket(&self.config.bucket_name)
            .key(s3_key)
            .send()
            .await?;

        let data = response.body.collect().await?;
        Ok(data.into_bytes().to_vec())
    }

    /// Delete a transcript chunk from S3
    pub async fn delete_transcript_chunk(&self, s3_key: &str) -> Result<(), TranscriptError> {
        debug!("Deleting transcript chunk from S3: {}", s3_key);

        let retry_strategy = ExponentialBackoff::from_millis(
            self.config.initial_retry_delay.as_millis() as u64
        )
        .max_delay(self.config.max_retry_delay)
        .take(self.config.max_retries);

        let delete_result = Retry::spawn(retry_strategy, || {
            self.delete_from_s3(s3_key)
        })
        .await;

        match delete_result {
            Ok(_) => {
                info!("Successfully deleted chunk from S3: {}", s3_key);
                Ok(())
            }
            Err(e) => {
                error!("Failed to delete chunk from S3 after {} retries: {} - {}",
                       self.config.max_retries, s3_key, e);
                Err(TranscriptError::S3UploadError(format!(
                    "Delete failed after retries: {}",
                    e
                )))
            }
        }
    }

    /// Check if S3 bucket is accessible
    pub async fn health_check(&self) -> Result<(), TranscriptError> {
        match self.client.head_bucket().bucket(&self.config.bucket_name).send().await {
            Ok(_) => {
                info!("S3 health check passed for bucket: {}", self.config.bucket_name);
                Ok(())
            }
            Err(e) => {
                error!("S3 health check failed for bucket {}: {}", self.config.bucket_name, e);
                Err(TranscriptError::S3UploadError(format!(
                    "S3 bucket not accessible: {}",
                    e
                )))
            }
        }
    }

    /// Delete data from S3
    async fn delete_from_s3(&self, s3_key: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.client
            .delete_object()
            .bucket(&self.config.bucket_name)
            .key(s3_key)
            .send()
            .await?;

        Ok(())
    }
}
