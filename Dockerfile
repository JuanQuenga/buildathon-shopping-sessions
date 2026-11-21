FROM rust:1.99.1-alpine@sha256:2c83bf1a38975801abfd05e92387242ab4d5f96fe05131e2a279f3009ad16393 AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY .cargo .cargo
COPY src src
RUN RUSTFLAGS="-C target-cpu=x86-64-v3 -C target-feature=+crt-static" cargo build --release --locked -j 4 \
 && cp target/release/pipeline /pipeline \
 && strip /pipeline

FROM scratch
COPY --from=build /pipeline /pipeline
ENV DATA_DIR=/data OUTPUT_DIR=/output SCRATCH_DIR=/scratch
USER 1000:1000
CMD ["/pipeline"]
