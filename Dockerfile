# Builder stage: Use Alpine-based Rust for musl
FROM rust:alpine3.20 as builder

WORKDIR /app

# Install C build tools for aws-lc-sys and static linking
RUN apk add --no-cache \
    build-base \
    clang-dev \
    musl-dev \
    linux-headers \
    pkgconfig \
    openssl-dev \
    cmake \
    nasm  # If FIPS or assembly tests needed; optional otherwise

# Add musl target early
RUN rustup target add x86_64-unknown-linux-musl

# Set env vars for static musl build and include paths (fixes header errors)
ENV RUSTFLAGS="-C linker=musl-gcc -C target-feature=+crt-static"
ENV C_INCLUDE_PATH="/usr/include"
ENV CPLUS_INCLUDE_PATH="/usr/include"
ENV LIBRARY_PATH="/usr/lib"

# Copy Cargo files for caching
COPY Cargo.toml ./

# Dummy src for deps
RUN mkdir src && echo "fn main() {}" > src/main.rs

# Build deps with target (caches musl artifacts)
RUN cargo build --release --target x86_64-unknown-linux-musl && rm -rf src

# Copy real src
COPY src ./src

# Final build (touch to trigger rebuild)
RUN touch src/main.rs && cargo build --release --target x86_64-unknown-linux-musl

# Runtime: Minimal Alpine
FROM alpine:3.20

# Only runtime essentials (HTTPS for APNs)
RUN apk add --no-cache ca-certificates tzdata && update-ca-certificates

# Non-root user
RUN addgroup -g 1001 -S appgroup && adduser -S appuser -u 1001 -G appgroup

WORKDIR /app

# Copy binary and key with ownership
COPY --from=builder --chown=appuser:appgroup /app/target/x86_64-unknown-linux-musl/release/serverAPNS ./serverAPNS
COPY --chown=appuser:appgroup key.p8 ./key.p8

# Switch user
USER appuser

EXPOSE 9090
ENV PORT=9090
ENV RUST_LOG=INFO

# Run (match binary name)
CMD ["/app/serverAPNS"]