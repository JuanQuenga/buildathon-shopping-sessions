# Shopping Sessions: native Rust pipeline. Multi-stage build, pinned base images.
# Build stage: Rust 1.99.1 (glibc, bookworm). Codegen flags come from .cargo/config.toml
# (x86-64-v3 baseline: AVX2/BMI2/FMA; the scorer is Cascade Lake or newer).
FROM rust:1.99.1-slim-bookworm@sha256:f2c004e3d2d8b547e8cf7ffbc29205385f4e71d23e53e4e383065db5e9f822cd AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY .cargo .cargo
COPY src src
RUN cargo build --release --locked \
 && cp target/release/pipeline /pipeline \
 && strip /pipeline

# Runtime: same glibc as the build stage, nothing else. The scorer runs as uid 1000 with a
# read-only root; the binary only writes under $OUTPUT_DIR.
FROM debian:bookworm-slim@sha256:734a72b3d6142a71993b4bc580ca01d8ca687f77a2c422d96a961d3376b179bc
COPY --from=build /pipeline /pipeline
ENV DATA_DIR=/data OUTPUT_DIR=/output SCRATCH_DIR=/scratch
USER 1000:1000
CMD ["/pipeline"]
