-- Rollback migration: Drop transcript tables and related objects
-- This migration removes all transcript-related database objects

-- Drop triggers first
DROP TRIGGER IF EXISTS update_transcript_sessions_updated_at ON transcript_sessions;

-- Drop function
DROP FUNCTION IF EXISTS update_updated_at_column();

-- Drop indexes
DROP INDEX IF EXISTS idx_transcript_chunks_created_at;
DROP INDEX IF EXISTS idx_transcript_chunks_s3_key;
DROP INDEX IF EXISTS idx_transcript_chunks_chunk_number;
DROP INDEX IF EXISTS idx_transcript_chunks_session_id;

DROP INDEX IF EXISTS idx_transcript_sessions_started_at;
DROP INDEX IF EXISTS idx_transcript_sessions_status;
DROP INDEX IF EXISTS idx_transcript_sessions_user_id;

-- Drop tables (chunks first due to foreign key constraint)
DROP TABLE IF EXISTS transcript_chunks;
DROP TABLE IF EXISTS transcript_sessions;
