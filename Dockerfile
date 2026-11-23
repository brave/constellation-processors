FROM rust:1-trixie@sha256:223cc32ddca8f3ee90c436dbc94b6b5f052ea7f9653c5201e9612f08ad7d21b8 AS builder

WORKDIR /usr/src/app

COPY ./src ./src
COPY ./migrations ./migrations
COPY ./Cargo.toml .
COPY ./Cargo.lock .

RUN cargo build --release

FROM debian:trixie-slim@sha256:a72b4433de1cf7d6bae0c4cee74d0547f58601a643b3375f070c51600d63b6b2

EXPOSE 8080 9090

WORKDIR /usr/src/app

# less is the paginator for aws stepfunctions cli
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    ca-certificates curl unzip less python3-minimal libpq5 libssl3t64 \
  && curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o awscliv2.zip \
  && unzip -q awscliv2.zip \
  && ./aws/install \
  && rm -rf ./aws awscliv2.zip \
  && apt-get purge -y --auto-remove curl unzip \
  && rm -rf /var/lib/apt/lists/*

COPY ./misc/rds_iam_bootstrap /usr/local/bin/
COPY --from=builder /usr/src/app/target/release/constellation-processors /usr/local/bin/

CMD ["constellation-processors"]
