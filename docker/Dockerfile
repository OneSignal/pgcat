FROM rust:1.67 AS builder
COPY . /app
WORKDIR /app
RUN cargo build --release

FROM ubuntu:22.04 AS release
WORKDIR /etc/pgcat
ENV RUST_LOG=info
RUN apt-get update && apt-get install  -o Dpkg::Options::=--force-confdef -yq --no-install-recommends \
    postgresql-client \
    # Clean up layer
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* \
    && truncate -s 0 /var/log/*log
COPY --from=builder /app/target/release/pgcat /usr/bin/pgcat
COPY --from=builder /app/pgcat.toml /etc/pgcat/pgcat.toml
EXPOSE 6432 9930
ENTRYPOINT ["pgcat", "/etc/pgcat/pgcat.toml"]
