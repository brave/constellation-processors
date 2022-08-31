all: migrate test build

build:
	cargo build --release

migrate:
	diesel migration run
	. ./.env && diesel migration run --database-url="$$TEST_DATABASE_URL"

test:
	cargo test

run:
	cargo run
