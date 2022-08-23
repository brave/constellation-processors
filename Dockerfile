FROM rust

WORKDIR /usr/src/app

COPY . .

RUN cargo install --path .

RUN cargo clean
RUN rm -rf /usr/local/cargo/registry

CMD ["nested-star-processors"]
