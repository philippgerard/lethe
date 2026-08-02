FROM rust:1.96-slim@sha256:31ee7fc65186be7e0e0ccb3f2ca305f14e4739e7642a1ae65753aa5d7b874523 AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        build-essential \
        pkg-config \
        protobuf-compiler \
        libprotobuf-dev \
        libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/lethe

COPY Cargo.toml Cargo.lock ./
COPY config/ config/
COPY src/ src/
COPY vendor/ vendor/

RUN cargo build --release

FROM debian:trixie-slim@sha256:020c0d20b9880058cbe785a9db107156c3c75c2ac944a6aa7ab59f2add76a7bd

# Lean base: just what the binary needs to boot (TLS), fetch installers, and
# the single most-common agent dependency (git). Everything heavier the agent
# installs on demand — it persists in the container's writable layer. Pass
# `--build-arg WITH_TOOLS=1` (e.g. `lethe container up --from-source --with-tools`)
# to bake the batteries-included set up front.
ARG WITH_TOOLS=0
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl git \
    && if [ "$WITH_TOOLS" = "1" ]; then \
         apt-get install -y --no-install-recommends \
           ffmpeg python3 python3-venv python3-pip build-essential \
           ripgrep jq unzip xz-utils file diffutils procps less which; \
       fi \
    && rm -rf /var/lib/apt/lists/*

# Run as root inside (rootless engine maps container-root → your host user),
# so the agent can install software and bind-mount writes are owned by you.
COPY --from=builder /opt/lethe/target/release/lethe /usr/local/bin/lethe

ENV HOME=/root LETHE_HOME=/root/.lethe
WORKDIR /root

ENTRYPOINT ["lethe"]
