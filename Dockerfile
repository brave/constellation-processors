FROM rust:1@sha256:4c45f61ebe054560190f232b7d883f174ff287e1a0972c8f6d7ab88da0188870

EXPOSE 8080 9090

WORKDIR /usr/src/app

RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
RUN unzip awscliv2.zip
RUN ./aws/install
RUN rm -rf ./aws

# install paginator for aws stepfunctions cli
RUN apt update && apt install -y less

COPY ./src ./src
COPY ./migrations ./migrations
COPY ./Cargo.toml .
COPY ./Cargo.lock .

COPY ./misc/rds_iam_bootstrap /usr/local/bin/

RUN cargo build --release
RUN cp ./target/release/constellation-processors /usr/local/bin/

RUN cargo clean
RUN rm -rf /usr/local/cargo/registry

CMD ["constellation-processors"]
