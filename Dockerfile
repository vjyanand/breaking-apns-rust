FROM alpine:latest

RUN apk add --update --no-cache --repository https://dl-3.alpinelinux.org/alpine/latest-stable/community --repository https://dl-3.alpinelinux.org/alpine/latest-stable/main rust cargo openssl-dev

WORKDIR /opt/breaking

# Copy Cargo files for caching
COPY Cargo.toml ./

# Dummy src for deps
RUN mkdir src && echo "fn main() {}" > src/main.rs

# Build deps with target (caches musl artifacts)
RUN cargo build --release && rm -rf src

# Copy real src
COPY src ./src

# Final build (touch to trigger rebuild)
RUN touch src/main.rs && cargo build --release

# Runtime: Minimal Alpine
FROM alpine:latest

RUN apk add --update --no-cache --repository https://dl-3.alpinelinux.org/alpine/latest-stable/community --repository https://dl-3.alpinelinux.org/alpine/latest-stable/main libgcc

WORKDIR /opt/breaking

COPY --from=0 /opt/breaking/target/release/serverAPNS ./

COPY key.p8 ./

ENV PORT 8080

EXPOSE 8080

ENV RUST_BACKTRACE=1

ENV RUST_LOG=info,reqwest=warn,hyper_util::client::legacy::connect::http=warn,hyper_util::client::legacy::pool=warn,hyper_util::client::legacy::connect::dns=warn

CMD ["./serverAPNS"]