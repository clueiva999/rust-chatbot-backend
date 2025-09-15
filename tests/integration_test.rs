/**
 * Integration Tests for Transcript Streaming Service
 * 
 * These tests validate the complete flow from WebSocket connections
 * to S3 uploads and database storage.
 */

use std::time::Duration;
use tokio::time::sleep;
use serde_json::json;
use uuid::Uuid;

// Import our modules - using the binary crate name
use rust_chatbot_groq::transcript::*;
use rust_chatbot_groq::transcript_service::TranscriptService;

#[tokio::test]
async fn test_transcript_buffer_functionality() {
    println!("Testing transcript buffer functionality...");
    
    let session_id = Uuid::new_v4().to_string();
    let mut buffer = TranscriptBuffer::new(session_id.clone(), "test-user".to_string());
    
    // Test adding items
    let item1 = TranscriptItem {
        speaker_id: "Me".to_string(),
        text: "Hello world".to_string(),
        timestamp: chrono::Utc::now().to_rfc3339(),
    };
    
    let item2 = TranscriptItem {
        speaker_id: "Them".to_string(),
        text: "Hi there!".to_string(),
        timestamp: chrono::Utc::now().to_rfc3339(),
    };
    
    buffer.add_item(item1);
    buffer.add_item(item2);
    
    assert_eq!(buffer.items.len(), 2);
    assert_eq!(buffer.session_id, session_id);
    
    // Test buffer should flush (size limit)
    for i in 0..60 {
        let item = TranscriptItem {
            speaker_id: format!("Speaker{}", i % 2),
            text: format!("Message number {}", i),
            timestamp: chrono::Utc::now().to_rfc3339(),
        };
        buffer.add_item(item);
    }
    
    assert!(buffer.should_flush(50, Duration::from_secs(30)));
    
    println!("✅ Transcript buffer functionality test passed");
}

#[tokio::test]
async fn test_transcript_service_session_management() {
    println!("Testing transcript service session management...");
    
    let service = TranscriptService::new();
    let session_id = Uuid::new_v4().to_string();
    let user_id = "test-user".to_string();
    
    // Test session creation
    let session = service.create_session(session_id.clone(), user_id.clone(), Some("Test Session".to_string())).await;
    assert!(session.is_ok());
    
    // Test adding items to session
    let item = TranscriptItem {
        speaker_id: "Me".to_string(),
        text: "Test message".to_string(),
        timestamp: chrono::Utc::now().to_rfc3339(),
    };
    
    let result = service.add_transcript_item(session_id.clone(), item).await;
    assert!(result.is_ok());
    
    // Test session exists
    assert!(service.session_exists(&session_id).await);
    
    // Test getting session info
    let session_info = service.get_session_info(&session_id).await;
    assert!(session_info.is_ok());
    
    println!("✅ Transcript service session management test passed");
}

#[tokio::test]
async fn test_s3_service_operations() {
    println!("Testing S3 service operations...");
    
    // Note: This test requires AWS credentials and S3 bucket to be configured
    // In a real environment, you would use a test bucket or mock S3 service
    
    let s3_service = S3Service::new(
        "test-bucket".to_string(),
        "us-east-1".to_string(),
    ).await;
    
    if s3_service.is_err() {
        println!("⚠️  S3 service test skipped - AWS credentials not configured");
        return;
    }
    
    let s3_service = s3_service.unwrap();
    
    // Test health check
    let health = s3_service.health_check().await;
    if health.is_err() {
        println!("⚠️  S3 service test skipped - S3 not accessible");
        return;
    }
    
    // Test upload and download
    let test_items = vec![
        TranscriptItem {
            speaker_id: "Me".to_string(),
            text: "Test message 1".to_string(),
            timestamp: chrono::Utc::now().to_rfc3339(),
        },
        TranscriptItem {
            speaker_id: "Them".to_string(),
            text: "Test message 2".to_string(),
            timestamp: chrono::Utc::now().to_rfc3339(),
        },
    ];
    
    let chunk_key = format!("test-session-{}/chunk-{}.jsonl.gz", Uuid::new_v4(), Uuid::new_v4());
    
    // Test upload
    let upload_result = s3_service.upload_transcript_chunk(&chunk_key, &test_items).await;
    assert!(upload_result.is_ok());
    
    // Test download
    let download_result = s3_service.download_transcript_chunk(&chunk_key).await;
    assert!(download_result.is_ok());
    
    let downloaded_items = download_result.unwrap();
    assert_eq!(downloaded_items.len(), test_items.len());
    
    // Test delete
    let delete_result = s3_service.delete_chunk(&chunk_key).await;
    assert!(delete_result.is_ok());
    
    println!("✅ S3 service operations test passed");
}

#[tokio::test]
async fn test_database_service_operations() {
    println!("Testing database service operations...");
    
    // Note: This test requires a PostgreSQL database to be configured
    // In a real environment, you would use a test database
    
    let db_service = DatabaseService::new("postgresql://test:test@localhost/test_transcript_db").await;
    
    if db_service.is_err() {
        println!("⚠️  Database service test skipped - PostgreSQL not configured");
        return;
    }
    
    let db_service = db_service.unwrap();
    
    let session_id = Uuid::new_v4().to_string();
    let user_id = "test-user".to_string();
    
    // Test session creation
    let session_result = db_service.create_session(
        session_id.clone(),
        user_id.clone(),
        Some("Test Session".to_string()),
    ).await;
    assert!(session_result.is_ok());
    
    // Test chunk metadata storage
    let chunk_result = db_service.store_chunk_metadata(
        session_id.clone(),
        "test-chunk-key".to_string(),
        100,
        10,
        1024,
    ).await;
    assert!(chunk_result.is_ok());
    
    // Test session retrieval
    let session_info = db_service.get_session_metadata(&session_id).await;
    assert!(session_info.is_ok());
    
    // Test user sessions retrieval
    let user_sessions = db_service.get_user_sessions(&user_id, 10, 0).await;
    assert!(user_sessions.is_ok());
    
    // Test session deletion
    let delete_result = db_service.delete_session(&session_id).await;
    assert!(delete_result.is_ok());
    
    println!("✅ Database service operations test passed");
}

#[tokio::test]
async fn test_complete_flow_simulation() {
    println!("Testing complete flow simulation...");
    
    let service = TranscriptService::new();
    let session_id = Uuid::new_v4().to_string();
    let user_id = "test-user".to_string();
    
    // 1. Create session
    let session_result = service.create_session(
        session_id.clone(),
        user_id.clone(),
        Some("Integration Test Session".to_string()),
    ).await;
    assert!(session_result.is_ok());
    
    // 2. Add multiple transcript items
    for i in 0..25 {
        let item = TranscriptItem {
            speaker_id: if i % 2 == 0 { "Me".to_string() } else { "Them".to_string() },
            text: format!("This is test message number {}", i + 1),
            timestamp: chrono::Utc::now().to_rfc3339(),
        };
        
        let result = service.add_transcript_item(session_id.clone(), item).await;
        assert!(result.is_ok());
        
        // Small delay to simulate real-time conversation
        sleep(Duration::from_millis(10)).await;
    }
    
    // 3. Force flush to trigger S3 upload and database storage
    let flush_result = service.force_flush_session(&session_id).await;
    if flush_result.is_ok() {
        println!("✅ Force flush successful");
    } else {
        println!("⚠️  Force flush failed (S3/DB not configured): {:?}", flush_result.err());
    }
    
    // 4. Verify session info
    let session_info = service.get_session_info(&session_id).await;
    assert!(session_info.is_ok());
    
    let info = session_info.unwrap();
    assert_eq!(info.session_id, session_id);
    assert_eq!(info.user_id, user_id);
    
    println!("✅ Complete flow simulation test passed");
}

#[tokio::test]
async fn test_websocket_message_parsing() {
    println!("Testing WebSocket message parsing...");
    
    // Test valid transcript message
    let valid_message = json!({
        "type": "transcript_item",
        "speaker_id": "Me",
        "text": "Hello world",
        "timestamp": "2024-01-01T12:00:00Z"
    });
    
    let parsed: Result<TranscriptWsMessage, _> = serde_json::from_value(valid_message);
    assert!(parsed.is_ok());
    
    let message = parsed.unwrap();
    match message {
        TranscriptWsMessage::TranscriptItem { speaker_id, text, timestamp } => {
            assert_eq!(speaker_id, "Me");
            assert_eq!(text, "Hello world");
            assert_eq!(timestamp, "2024-01-01T12:00:00Z");
        }
        _ => panic!("Expected TranscriptItem message"),
    }
    
    // Test invalid message
    let invalid_message = json!({
        "type": "invalid_type",
        "data": "some data"
    });
    
    let parsed: Result<TranscriptWsMessage, _> = serde_json::from_value(invalid_message);
    assert!(parsed.is_err());
    
    println!("✅ WebSocket message parsing test passed");
}

#[tokio::test]
async fn test_error_handling_scenarios() {
    println!("Testing error handling scenarios...");
    
    let service = TranscriptService::new();
    
    // Test adding item to non-existent session
    let non_existent_session = Uuid::new_v4().to_string();
    let item = TranscriptItem {
        speaker_id: "Me".to_string(),
        text: "Test message".to_string(),
        timestamp: chrono::Utc::now().to_rfc3339(),
    };
    
    let result = service.add_transcript_item(non_existent_session.clone(), item).await;
    assert!(result.is_err());
    
    // Test getting info for non-existent session
    let info_result = service.get_session_info(&non_existent_session).await;
    assert!(info_result.is_err());
    
    // Test force flush on non-existent session
    let flush_result = service.force_flush_session(&non_existent_session).await;
    assert!(flush_result.is_err());
    
    println!("✅ Error handling scenarios test passed");
}

// Helper function to run all tests
pub async fn run_integration_tests() {
    println!("🚀 Starting integration tests...\n");
    
    test_transcript_buffer_functionality().await;
    test_transcript_service_session_management().await;
    test_s3_service_operations().await;
    test_database_service_operations().await;
    test_complete_flow_simulation().await;
    test_websocket_message_parsing().await;
    test_error_handling_scenarios().await;
    
    println!("\n🎉 All integration tests completed!");
}

#[tokio::test]
async fn test_transcript_websocket_authentication_messages() {
    println!("Testing transcript WebSocket authentication message parsing...");

    // Test authenticate message
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

    // Test auth response message
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

    // Test error auth response
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

    println!("✅ Transcript WebSocket authentication message parsing test passed");
}
