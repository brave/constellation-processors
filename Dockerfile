FROM rust

WORKDIR /usr/src/app

COPY . .

# Remove once nested-sta-rs goes public
RUN sed -i 's/git = "ssh:\/\/git@github\.com\/brave\-experiments\/nested-sta-rs", branch = "main"/path = ".\/nested\-sta\-rs"/g' Cargo.toml

RUN cargo install --path .

RUN cargo clean
RUN rm -rf /usr/local/cargo/registry

CMD ["nested-star-aggregator"]
