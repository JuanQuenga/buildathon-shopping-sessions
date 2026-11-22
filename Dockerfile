# Shopping Sessions: native Rust pipeline. Multi-stage build, pinned base images.
# Build stage: Rust 1.99.1 (glibc, bookworm) with self-contained 2-pass LLVM PGO
# targeting x86-64-v3 (AVX2/BMI2/FMA; the scorer is Cascade Lake or newer).
FROM rust:1.99.1-slim-bookworm@sha256:f2c004e3d2d8b547e8cf7ffbc29205385f4e71d23e53e4e383065db5e9f822cd AS build
RUN rustup component add llvm-tools-preview
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY .cargo .cargo
COPY src src
RUN RUSTFLAGS="-Ctarget-cpu=x86-64-v3 -Cprofile-generate=/tmp/pgo" cargo build --release --locked -j 4 \
 && mkdir -p /tmp/pgo-data /tmp/pgo-out \
 && INGEST_GEN_PGO=/tmp/pgo-data target/release/pipeline \
 && DATA_DIR=/tmp/pgo-data OUTPUT_DIR=/tmp/pgo-out NCPU=4 INGEST_SINK_THP=0 target/release/pipeline \
 && "$(rustc --print sysroot)/lib/rustlib/x86_64-unknown-linux-gnu/bin/llvm-profdata" merge -o /tmp/pgo.profdata /tmp/pgo \
 && RUSTFLAGS="-Ctarget-cpu=x86-64-v3 -Cprofile-use=/tmp/pgo.profdata" cargo build --release --locked -j 4 \
 && cp target/release/pipeline /pipeline \
 && strip /pipeline \
 && rm -rf /tmp/pgo /tmp/pgo-data /tmp/pgo-out /tmp/pgo.profdata

# Runtime: same glibc as the build stage, nothing else. The scorer runs as uid 1000 with a
# read-only root; the binary only writes under $OUTPUT_DIR.
FROM debian:bookworm-slim@sha256:734a72b3d6142a71993b4bc580ca01d8ca687f77a2c422d96a961d3376b179bc
COPY --from=build /pipeline /pipeline
ENV DATA_DIR=/data OUTPUT_DIR=/output SCRATCH_DIR=/scratch
USER 1000:1000
CMD ["/pipeline"]
