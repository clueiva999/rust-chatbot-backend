# Multi-stage Dockerfile for Rust Transcript Streaming Backend
# Optimized for both development and production builds

# =============================================================================
# Build Stage
# =============================================================================
FROM rust:1.70-slim as builder

# Install system dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /app

# Copy dependency files first for better caching
COPY Cargo.toml Cargo.lock ./

# Create a dummy main.rs to build dependencies
RUN mkdir src && echo "fn main() {}" > src/main.rs

# Build dependencies (this layer will be cached unless Cargo.toml changes)
RUN cargo build --release && rm -rf src

# Copy source code
COPY src ./src
COPY migrations ./migrations

# Build the application
RUN cargo build --release

# =============================================================================
# Development Stage
# =============================================================================
FROM rust:1.70-slim as development

# Install system dependencies and development tools
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    libpq-dev \
    curl \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Install cargo-watch for hot reloading
RUN cargo install cargo-watch

# Create app directory
WORKDIR /app

# Copy source code
COPY . .

# Expose port
EXPOSE 8080

# Development command with hot reloading
CMD ["cargo", "watch", "-x", "run"]

# =============================================================================
# Production Stage
# =============================================================================
FROM debian:bookworm-slim as production

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    libpq5 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user for security
RUN groupadd -r transcript && useradd -r -g transcript transcript

# Create app directory
WORKDIR /app

# Copy the binary from builder stage
COPY --from=builder /app/target/release/rust-chatbot-groq /usr/local/bin/transcript-service

# Copy migrations
COPY --from=builder /app/migrations ./migrations

# Create logs directory
RUN mkdir -p /app/logs && chown -R transcript:transcript /app

# Switch to non-root user
USER transcript

# Expose port
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# Production command
CMD ["transcript-service"]

# =============================================================================
# Default target
# =============================================================================
FROM production as default
