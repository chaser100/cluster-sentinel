# syntax=docker/dockerfile:1.7

FROM rust:1.89-bookworm AS builder
WORKDIR /src
RUN apt-get update \
    && apt-get install -y --no-install-recommends pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked

FROM gcr.io/distroless/cc-debian12:nonroot
ARG BUILD_VERSION=dev
ARG VCS_REF=unknown
WORKDIR /app
COPY --from=builder /src/target/release/clustersentinel /app/clustersentinel
ENV CLUSTERSENTINEL_BUILD_VERSION=${BUILD_VERSION} \
    CLUSTERSENTINEL_GIT_SHA=${VCS_REF}
LABEL org.opencontainers.image.title="Cluster Sentinel" \
      org.opencontainers.image.description="Kubernetes event collector with Prometheus metrics and an embedded MCP server" \
      org.opencontainers.image.source="https://github.com/chaser100/cluster-sentinel" \
      org.opencontainers.image.version="${BUILD_VERSION}" \
      org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.licenses="MIT"
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/app/clustersentinel"]
