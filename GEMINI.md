# retina

**High-level RTSP multimedia streaming library in Rust.**

This project is a Rust library for RTSP multimedia streaming, designed with a focus on ONVIF RTSP/1.0 IP surveillance cameras. It works around common issues found in cheap closed-source cameras and is used in production by [Moonfire NVR](https://github.com/scottlamb/moonfire-nvr).

## Project Overview

*   **Language:** Rust (Edition 2024)
*   **Core Logic:** Async-first using `tokio`.
*   **Key Features:**
    *   **RTSP Client:** Supports RTSP 1.0, Basic/Digest auth.
    *   **Transport:** RTP over TCP (interleaved) and experimental RTP over UDP.
    *   **Codecs (Depacketization):** H.264, H.265, MJPEG, AAC, G.711, G.723.
    *   **ONVIF:** Support for ONVIF metadata.
*   **Status:** Active development, production-ready core features, but some planned features (RTSP 2.0, Server support) are pending.

## Building and Running

The project is structured as a Cargo workspace.

### Core Library

To build the library:
```bash
cargo build
```

To run unit tests:
```bash
cargo test
```

To run benchmarks:
```bash
cargo bench
```

### Examples

The `examples/` directory contains practical usage examples.

**1. CLI Client (`examples/client`)**
General-purpose RTSP client tool.

*   **Info:** Get stream details.
    ```bash
    cargo run --package client info --url rtsp://user:pass@ip/stream
    ```
*   **Record to MP4:**
    ```bash
    cargo run --package client mp4 --url rtsp://user:pass@ip/stream output.mp4
    ```
*   **ONVIF Metadata:**
    ```bash
    cargo run --package client onvif --url rtsp://user:pass@ip/stream
    ```
*   **Save JPEGs:**
    ```bash
    cargo run --package client jpeg --url rtsp://user:pass@ip/stream
    ```

**2. WebRTC Proxy (`examples/webrtc-proxy`)**
Proxies H.264 RTSP stream to WebRTC for browser viewing.
```bash
cargo run --package webrtc-proxy -- --help
```

**3. FFmpeg Decode (`examples/ffmpeg-decode`)**
Decodes RTSP stream to raw frames using `ffmpeg-next`.

## Architecture

### Codecs & Depacketization

The `Depacketizer` (`src/codec/mod.rs`) is a core component responsible for reassembling RTP packets into usable media frames (Access Units).

*   **Design:**
    *   `Depacketizer` is a wrapper enum around specific codec implementations (`H264`, `H265`, `AAC`, `MJPEG`, etc.).
    *   **Factory Pattern:** `Depacketizer::new` instantiates the correct internal handler based on SDP media attributes (media type, encoding name).
    *   **Data Flow:**
        *   `push(packet)`: Feeds an RTP packet. State is updated internally.
        *   `pull()`: Returns `Ok(Some(CodecItem))` when a full frame is ready (e.g., after an RTP Marker bit or timestamp change).
        *   `parameters()`: Returns stream metadata (SPS/PPS for video, sample rate for audio).

*   **H.264 Implementation (`src/codec/h264.rs`):**
    *   **State Machine:** Tracks assembly state (`New`, `PreMark`, `PostMark`, `Loss`) to handle packet loss and frame boundaries robustly.
    *   **Assembly:** Handles Single NALs, STAP-A (Aggregation), and FU-A (Fragmentation).
    *   **Robustness:** Includes logic to handle non-compliant streams, such as those embedding Annex B start codes (`00 00 01`) within RTP payloads.
    *   **Output:** Produces `VideoFrame`s in AVCC format (length-prefixed) suitable for MP4 containers.

## Development Conventions

*   **Style:** Follows standard Rust idioms.
*   **Linting:** Uses `clippy`. Check `clippy.toml` for specific configuration.
    ```bash
    cargo clippy
    ```
*   **Async:** Heavy usage of `tokio` and `futures`. Ensure non-blocking I/O in new code.
*   **Error Handling:** Uses `thiserror` for library errors.
*   **Dependencies:** Key dependencies include `rtsp-types`, `h264-reader`, `bytes`, and `tokio`.

## Directory Structure

*   `src/`: Core library source code.
    *   `client/`: RTSP client implementation.
    *   `codec/`: Codec-specific depacketization logic (H.264, H.265, etc.).
*   `examples/`: Standalone binaries demonstrating library usage.
*   `benches/`: Performance benchmarks.
*   `fuzz/`: Fuzz testing targets.
