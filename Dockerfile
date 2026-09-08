# syntax=docker/dockerfile:1.7

FROM rust:1.89-bookworm@sha256:948f9b08a66e7fe01b03a98ef1c7568292e07ec2e4fe90d88c07bb14563c84ff AS builder
ARG TARGETARCH
ARG CARGO_BUILD_JOBS=1
ARG CARGO_PROFILE_RELEASE_LTO=thin
ARG MUSL_TOOLS_VERSION=1.2.3-1
WORKDIR /src
RUN apt-get update \
    && apt-get install -y --no-install-recommends "musl-tools=${MUSL_TOOLS_VERSION}" \
    && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN case "${TARGETARCH:-$(uname -m)}" in \
      amd64|x86_64) rust_target="x86_64-unknown-linux-musl" ;; \
      arm64|aarch64) rust_target="aarch64-unknown-linux-musl" ;; \
      *) echo "unsupported target architecture: ${TARGETARCH:-$(uname -m)}" >&2; exit 1 ;; \
    esac \
    && rustup target add "$rust_target" \
    && cargo build --release --locked --jobs "$CARGO_BUILD_JOBS" --target "$rust_target" \
    && cp "target/$rust_target/release/clustersentinel" /src/clustersentinel

FROM gcr.io/distroless/static-debian13:nonroot@sha256:1c2c046bc09ed40fad370b599a0b1ae7987f55b01e247cf27a7c27cd97e5bbc7
ARG BUILD_VERSION=dev
ARG VCS_REF=unknown
WORKDIR /app
COPY --from=builder /src/clustersentinel /app/clustersentinel
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
