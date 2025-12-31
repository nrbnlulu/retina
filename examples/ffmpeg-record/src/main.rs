// Copyright (C) 2025
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Records an RTSP stream to an MP4 file using ffmpeg-next.
//!
//! This example receives already-encoded video frames from retina and muxes them
//! directly into an MP4 container using ffmpeg's libavformat.
//!
//! Usage:
//!   cargo run --package ffmpeg-record -- \
//!     --url rtsp://user:pass@ip/stream \
//!     --output recording.mp4 \
//!     --duration 30

extern crate ffmpeg_next as ffmpeg;

use anyhow::{Context, Error, anyhow, bail};
use clap::Parser;
use futures::StreamExt;
use log::{error, info, warn};
use retina::{
    client::SetupOptions,
    codec::{CodecItem, ParametersRef, VideoParameters},
};
use std::{path::PathBuf, str::FromStr, sync::Arc, time::Duration};

/// Records an RTSP stream to an MP4 file using ffmpeg.
#[derive(Parser)]
struct Opts {
    /// `rtsp://` URL to connect to.
    #[clap(long)]
    url: url::Url,

    /// Username for RTSP authentication.
    #[clap(long)]
    username: Option<String>,

    /// Password for RTSP authentication.
    #[clap(long, requires = "username")]
    password: Option<String>,

    /// Output file path (e.g., recording.mp4).
    #[clap(long, short)]
    output: PathBuf,

    /// Recording duration in seconds (0 = unlimited, stop with Ctrl+C).
    #[clap(long, short, default_value = "0")]
    duration: u64,

    /// Transport protocol: `tcp` or `udp`.
    #[clap(long, default_value_t)]
    transport: retina::client::Transport,

    /// When to issue a `TEARDOWN` request: `auto`, `always`, or `never`.
    #[clap(long, default_value_t)]
    teardown: retina::client::TeardownPolicy,

    /// Allow packet loss without aborting.
    #[clap(long)]
    allow_loss: bool,
}

fn init_logging() -> mylog::Handle {
    let h = mylog::Builder::new()
        .format(
            std::env::var("MOONFIRE_FORMAT")
                .map_err(|_| ())
                .and_then(|s| mylog::Format::from_str(&s))
                .unwrap_or(mylog::Format::Google),
        )
        .spec(std::env::var("MOONFIRE_LOG").as_deref().unwrap_or("info"))
        .build();
    h.clone().install().unwrap();
    h
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VideoCodec {
    H264,
    H265,
    Mjpeg,
}

impl VideoCodec {
    fn ffmpeg_id(self) -> ffmpeg::codec::Id {
        match self {
            VideoCodec::H264 => ffmpeg::codec::Id::H264,
            VideoCodec::H265 => ffmpeg::codec::Id::HEVC,
            VideoCodec::Mjpeg => ffmpeg::codec::Id::MJPEG,
        }
    }
}

/// MP4 recorder using ffmpeg's muxer
struct Recorder {
    output_ctx: ffmpeg::format::context::Output,
    video_stream_index: usize,
    frame_count: u64,
    start_time: Option<std::time::Instant>,
    first_pts: Option<i64>,
    allow_loss: bool,
    header_written: bool,
}

impl Recorder {
    fn new(
        output_path: &std::path::Path,
        codec: VideoCodec,
        params: &VideoParameters,
        allow_loss: bool,
    ) -> Result<Self, Error> {
        ffmpeg::init().context("Failed to initialize ffmpeg")?;

        // Enable verbose logging for debugging
        ffmpeg::util::log::set_level(ffmpeg::util::log::Level::Warning);

        let output_path_str = output_path
            .to_str()
            .ok_or_else(|| anyhow!("Invalid output path"))?;

        // Create output context - ffmpeg will determine format from extension
        let mut output_ctx =
            ffmpeg::format::output(output_path_str).context("Failed to create output context")?;

        // Get the codec
        let codec_id = codec.ffmpeg_id();

        // Create a new stream in the output
        let mut stream = output_ctx
            .add_stream(ffmpeg::codec::encoder::find(codec_id))
            .context("Failed to add stream")?;

        let video_stream_index = stream.index();

        // Set up codec parameters for the stream
        let (width, height) = params.pixel_dimensions();
        let extra_data = params.extra_data();

        info!(
            "Video parameters: dimensions={}x{}, extradata={} bytes, codec={:?}",
            width,
            height,
            extra_data.len(),
            params.rfc6381_codec()
        );

        // Check if we have valid dimensions
        if width == 0 || height == 0 {
            bail!(
                "Video parameters have invalid dimensions ({}x{}). \
                 This may happen if the stream parameters are not yet available. \
                 Try waiting for more frames.",
                width,
                height
            );
        }

        // Access codec parameters through the stream
        unsafe {
            let codecpar = (*stream.as_mut_ptr()).codecpar;
            (*codecpar).codec_type = ffmpeg::ffi::AVMediaType::AVMEDIA_TYPE_VIDEO;
            (*codecpar).codec_id = codec_id.into();
            (*codecpar).width = width as i32;
            (*codecpar).height = height as i32;

            // Set extradata (SPS/PPS for H.264, VPS/SPS/PPS for H.265)
            if !extra_data.is_empty() {
                let extradata_size = extra_data.len();
                let extradata = ffmpeg::ffi::av_malloc(
                    extradata_size + ffmpeg::ffi::AV_INPUT_BUFFER_PADDING_SIZE as usize,
                ) as *mut u8;
                if extradata.is_null() {
                    bail!("Failed to allocate extradata");
                }
                std::ptr::copy_nonoverlapping(extra_data.as_ptr(), extradata, extradata_size);
                (*codecpar).extradata = extradata;
                (*codecpar).extradata_size = extradata_size as i32;
            }
        }

        // Time base: use 90kHz which is standard for video RTP
        stream.set_time_base(ffmpeg::Rational::new(1, 90000));

        info!(
            "Created output stream: codec={:?}, {}x{}, extradata={} bytes",
            codec,
            width,
            height,
            extra_data.len()
        );

        Ok(Self {
            output_ctx,
            video_stream_index,
            frame_count: 0,
            start_time: None,
            first_pts: None,
            allow_loss,
            header_written: false,
        })
    }

    fn ensure_header_written(&mut self) -> Result<(), Error> {
        if !self.header_written {
            self.output_ctx
                .write_header()
                .context("Failed to write header")?;
            self.header_written = true;
            info!("Wrote MP4 header");
        }
        Ok(())
    }

    fn push_frame(&mut self, frame: retina::codec::VideoFrame) -> Result<(), Error> {
        if self.start_time.is_none() {
            self.start_time = Some(std::time::Instant::now());
            info!("First frame received");
        }

        // Check for packet loss
        if frame.loss() > 0 {
            if self.allow_loss {
                warn!("Packet loss detected: {} packets", frame.loss());
            } else {
                bail!(
                    "Packet loss detected: {} packets (use --allow-loss to continue)",
                    frame.loss()
                );
            }
        }

        // Ensure header is written before any packets
        self.ensure_header_written()?;

        let timestamp = frame.timestamp();
        let is_keyframe = frame.is_random_access_point();
        let data = frame.into_data();

        // Calculate PTS - use timestamp from retina (90kHz clock)
        let pts = timestamp.timestamp();

        // Normalize PTS to start from 0
        let pts = if let Some(first) = self.first_pts {
            pts - first
        } else {
            self.first_pts = Some(pts);
            0
        };

        // Create packet
        let mut packet = ffmpeg::codec::packet::Packet::copy(&data);
        packet.set_stream(self.video_stream_index);
        packet.set_pts(Some(pts));
        packet.set_dts(Some(pts)); // For simplicity, DTS = PTS (works for most streams)

        // Set keyframe flag
        if is_keyframe {
            packet.set_flags(ffmpeg::codec::packet::Flags::KEY);
        }

        // Write packet
        packet
            .write_interleaved(&mut self.output_ctx)
            .context("Failed to write packet")?;

        self.frame_count += 1;

        if self.frame_count % 100 == 0 {
            let elapsed = self.start_time.map(|t| t.elapsed()).unwrap_or_default();
            info!(
                "Wrote {} frames ({:.1}s elapsed)",
                self.frame_count,
                elapsed.as_secs_f64()
            );
        }

        Ok(())
    }

    fn finish(mut self) -> Result<(), Error> {
        info!("Finishing recording ({} frames)...", self.frame_count);

        if self.header_written {
            // Write trailer to finalize the file
            self.output_ctx
                .write_trailer()
                .context("Failed to write trailer")?;
            info!("Wrote MP4 trailer");
        }

        let elapsed = self.start_time.map(|t| t.elapsed()).unwrap_or_default();
        info!(
            "Recording complete: {} frames in {:.1}s",
            self.frame_count,
            elapsed.as_secs_f64()
        );

        Ok(())
    }
}

#[tokio::main]
async fn main() {
    let mut h = init_logging();
    if let Err(e) = {
        let _a = h.async_scope();
        run().await
    } {
        error!("{:#}", e);
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Error> {
    let opts = Opts::parse();

    let creds = match (opts.username, opts.password) {
        (Some(username), password) => Some(retina::client::Credentials {
            username,
            password: password.unwrap_or_default(),
        }),
        (None, None) => None,
        _ => unreachable!(),
    };

    // Set up stop signal
    let stop_signal = tokio::signal::ctrl_c();
    tokio::pin!(stop_signal);

    // Set up duration timeout
    let duration = if opts.duration > 0 {
        info!("Recording for {} seconds", opts.duration);
        Some(Duration::from_secs(opts.duration))
    } else {
        info!("Recording until Ctrl+C...");
        None
    };
    let deadline = duration.map(|d| tokio::time::Instant::now() + d);

    // Connect to RTSP server
    info!("Connecting to {}...", opts.url);
    let session_group = Arc::new(retina::client::SessionGroup::default());
    let mut session = retina::client::Session::describe(
        opts.url.clone(),
        retina::client::SessionOptions::default()
            .creds(creds)
            .session_group(session_group.clone())
            .user_agent("Retina ffmpeg-record example".to_owned())
            .teardown(opts.teardown),
    )
    .await
    .context("Failed to connect to RTSP server")?;

    // Find video stream
    let (video_stream_i, codec) = session
        .streams()
        .iter()
        .enumerate()
        .find_map(|(i, s)| {
            if s.media() == "video" {
                match s.encoding_name() {
                    "h264" => Some((i, VideoCodec::H264)),
                    "h265" => Some((i, VideoCodec::H265)),
                    "jpeg" => Some((i, VideoCodec::Mjpeg)),
                    other => {
                        warn!("Ignoring unsupported video codec: {}", other);
                        None
                    }
                }
            } else {
                None
            }
        })
        .ok_or_else(|| anyhow!("No supported video stream found"))?;

    info!("Found {:?} video stream at index {}", codec, video_stream_i);

    // Setup stream
    session
        .setup(
            video_stream_i,
            SetupOptions::default().transport(opts.transport),
        )
        .await
        .context("Failed to setup stream")?;

    // Play with demuxing enabled
    let mut session = session
        .play(retina::client::PlayOptions::default())
        .await
        .context("Failed to start playback")?
        .demuxed()
        .context("Failed to demux stream")?;

    // Wait for initial parameters before creating recorder
    let mut recorder: Option<Recorder> = None;

    info!("Starting to record to {:?}...", opts.output);

    // Main loop
    loop {
        let timeout_future = async {
            if let Some(dl) = deadline {
                tokio::time::sleep_until(dl).await;
            } else {
                std::future::pending::<()>().await;
            }
        };

        tokio::select! {
            biased;

            _ = &mut stop_signal => {
                info!("Received Ctrl+C, stopping...");
                break;
            }

            _ = timeout_future => {
                info!("Recording duration reached");
                break;
            }

            item = session.next() => {
                match item {
                    Some(Ok(CodecItem::VideoFrame(frame))) => {
                        // Initialize recorder on first frame with valid parameters
                        if recorder.is_none() {
                            // Only initialize when we have valid parameters
                            // For H.265, parameters become valid after receiving SPS/PPS in the stream
                            let params = match session.streams()[video_stream_i].parameters() {
                                Some(ParametersRef::Video(p)) => p,
                                _ => {
                                    info!("No video parameters yet, skipping frame");
                                    continue;
                                }
                            };

                            // Check if dimensions are valid
                            let (w, h) = params.pixel_dimensions();
                            if w == 0 || h == 0 {
                                info!(
                                    "Video parameters have zero dimensions ({}x{}), waiting for valid parameters...",
                                    w, h
                                );
                                continue;
                            }

                            info!(
                                "Got valid video parameters: {}x{}, creating recorder",
                                w, h
                            );
                            recorder = Some(Recorder::new(&opts.output, codec, params, opts.allow_loss)?);
                        }

                        if let Some(ref mut rec) = recorder {
                            rec.push_frame(frame)?;
                        }
                    }
                    Some(Ok(CodecItem::AudioFrame(_))) => {
                        // Skip audio frames for now
                    }
                    Some(Ok(_)) => {
                        // Skip other items
                    }
                    Some(Err(e)) => {
                        return Err(anyhow!("RTSP error: {}", e));
                    }
                    None => {
                        info!("Stream ended");
                        break;
                    }
                }
            }
        }
    }

    // Finish recording
    if let Some(rec) = recorder {
        rec.finish()?;
    } else {
        warn!("No frames were recorded");
    }

    Ok(())
}
