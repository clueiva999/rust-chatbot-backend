# Database Migrations

This directory contains SQL migration files for the transcript streaming service.

## Migration Files

- `001_transcript_tables.sql` - Creates the transcript_sessions and transcript_chunks tables
- `001_transcript_tables_down.sql` - Rollback migration to drop the tables

## Running Migrations

### Using sqlx-cli (Recommended)

1. Install sqlx-cli:
```bash
cargo install sqlx-cli --no-default-features --features postgres
```

2. Set your database URL:
```bash
export DATABASE_URL="postgresql://username:password@localhost/database_name"
```

3. Run migrations:
```bash
sqlx migrate run
```

4. Rollback migrations:
```bash
sqlx migrate revert
```

### Manual Execution

You can also run the SQL files directly against your PostgreSQL database:

```bash
psql $DATABASE_URL -f migrations/001_transcript_tables.sql
```

## Schema Overview

### transcript_sessions
- Stores metadata about transcript sessions
- Links to user_id for authentication
- Tracks session status (active, completed, etc.)

### transcript_chunks
- Stores metadata about S3 chunks for each session
- References transcript_sessions via foreign key
- Tracks chunk ordering, file sizes, and timestamps
- Enables reassembly of complete transcripts

## Environment Variables

Make sure to set the following environment variables:

- `DATABASE_URL` - PostgreSQL connection string
- `AWS_ACCESS_KEY_ID` - AWS access key for S3
- `AWS_SECRET_ACCESS_KEY` - AWS secret key for S3
- `AWS_REGION` - AWS region for S3 bucket
- `S3_BUCKET_NAME` - S3 bucket name for storing transcript chunks
