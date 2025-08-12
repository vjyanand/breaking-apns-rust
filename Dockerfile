# Use the official Rust image as a build stage
FROM rust:latest as builder

# Set the working directory inside the container
WORKDIR /app

# Copy the Cargo.toml and Cargo.lock files first for better caching
COPY Cargo.toml ./

# Create a dummy main.rs to build dependencies
RUN mkdir src && echo "fn main() {}" > src/main.rs

# Build dependencies (this layer will be cached if Cargo files don't change)
RUN cargo build --release && rm -rf src

# Copy the actual source code
COPY src ./src

# Build the application
RUN touch src/main.rs && cargo build --release

# Use a smaller base image for the final stage
FROM debian:bookworm-slim

# Install necessary runtime dependencies
RUN apt-get update && apt-get install -y ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create a non-root user
RUN useradd -r -s /bin/false appuser

# Set the working directory
WORKDIR /app

# Copy the built binary from the builder stage
COPY --from=builder /app/target/release/serverAPNS ./app

COPY key.p8 /app

# Change ownership to the non-root user
RUN chown appuser:appuser ./app

# Switch to the non-root user
USER appuser

# Expose port 9090
EXPOSE 9090

ENV PORT=9090

ENV RUST_LOG=INFO

# Run the application
CMD ["./app"]

#apt-get update && apt-get install -y heaptrack procps gdb 