-- Setup script for transcript streaming database
-- Run this manually in your PostgreSQL database

-- Create database if it doesn't exist
-- CREATE DATABASE transcript_db;

-- Connect to the database and create tables
-- \c transcript_db;

-- Create transcript_sessions table
CREATE TABLE IF NOT EXISTS transcript_sessions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id VARCHAR(100) NOT NULL,
    session_name VARCHAR(255),
    status VARCHAR(50) DEFAULT 'active',
    started_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    ended_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create transcript_chunks table
CREATE TABLE IF NOT EXISTS transcript_chunks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id UUID NOT NULL REFERENCES transcript_sessions(id) ON DELETE CASCADE,
    chunk_number INTEGER NOT NULL,
    s3_bucket VARCHAR(100) NOT NULL,
    s3_key VARCHAR(500) NOT NULL,
    item_count INTEGER NOT NULL,
    file_size_bytes BIGINT NOT NULL,
    start_timestamp TIMESTAMP WITH TIME ZONE,
    end_timestamp TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(session_id, chunk_number)
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_transcript_sessions_user_id ON transcript_sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_transcript_sessions_status ON transcript_sessions(status);
CREATE INDEX IF NOT EXISTS idx_transcript_sessions_started_at ON transcript_sessions(started_at);

CREATE INDEX IF NOT EXISTS idx_transcript_chunks_session_id ON transcript_chunks(session_id);
CREATE INDEX IF NOT EXISTS idx_transcript_chunks_chunk_number ON transcript_chunks(chunk_number);
CREATE INDEX IF NOT EXISTS idx_transcript_chunks_s3_key ON transcript_chunks(s3_key);
CREATE INDEX IF NOT EXISTS idx_transcript_chunks_created_at ON transcript_chunks(created_at);

-- Create a function to automatically update the updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create trigger to automatically update updated_at on transcript_sessions
CREATE TRIGGER update_transcript_sessions_updated_at
    BEFORE UPDATE ON transcript_sessions
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Grant permissions (adjust as needed)
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO transcript_user;
-- GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO transcript_user;
