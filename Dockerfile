# Stage 1: Build static/minimal Rust binary
FROM rust:1-alpine AS builder
RUN apk add --no-cache musl-dev gcc
WORKDIR /app

# Limit parallel compiler jobs to minimize peak RAM usage on small instances
ENV CARGO_BUILD_JOBS=1

# Cache dependencies
COPY Cargo.toml ./
RUN mkdir src && echo "fn main() {}" > src/main.rs && cargo build --release && rm -rf src

# Copy application source and build release binary
COPY src ./src
RUN touch src/main.rs && cargo build --release

# Stage 2: Minimal Runtime Image (~10MB base)
FROM alpine:3.20
WORKDIR /app

# Create data directory for SQLite persistence
RUN mkdir -p /app/data

# Copy compiled binary and static frontend assets
COPY --from=builder /app/target/release/pollster-rust /app/pollster-rust
COPY static /app/static

ENV PORT=8085
ENV DATABASE_URL=/app/data/pollster.db
EXPOSE 8085

ENTRYPOINT ["/app/pollster-rust"]
