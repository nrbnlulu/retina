// Copyright (C) 2025
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Records an RTSP stream to an MP4 file using GStreamer.
//!
//! This example receives raw RTP packets from retina and uses GStreamer's
//! rtph264depay/rtph265depay to depacketize them, then muxes into fragmented MP4.
//!
//! Usage:
//!   cargo run --package gst-record -- \
//!     --url rtsp://user:pass@ip/stream \
//!     --output recording.mp4 \
//!     --duration 30

use anyhow::{Context, Result, anyhow, bail};
use clap::Parser;
use futures::StreamExt;
use gstreamer as gst;
use gstreamer::prelude::*;
use gstreamer_app as gst_app;
use log::{error, info, warn};
use retina::client::SetupOptions;
use std::{path::PathBuf, str::FromStr, sync::Arc, time::Duration};

/// Records an RTSP stream to a fragmented MP4 file using GStreamer's isofmp4mux.
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
}

/// GStreamer recording pipeline that receives raw RTP packets
struct Recorder {
    pipeline: gst::Pipeline,
    appsrc: gst_app::AppSrc,
    packet_count: u64,
    start_time: Option<std::time::Instant>,
    _bus_watch: gst::bus::BusWatchGuard,
}

impl Recorder {
    fn new(output_path: &std::path::Path, codec: VideoCodec, clock_rate: u32) -> Result<Self> {
        gst::init().context("Failed to initialize GStreamer")?;

        let output_path_str = output_path
            .to_str()
            .ok_or_else(|| anyhow!("Invalid output path"))?;

        let pipeline = gst::Pipeline::new();

        // AppSrc - receives raw RTP packets
        let appsrc = gst::ElementFactory::make("appsrc")
            .name("src")
            .build()
            .context("Failed to create appsrc")?
            .downcast::<gst_app::AppSrc>()
            .map_err(|_| anyhow!("Failed to downcast to AppSrc"))?;

        appsrc.set_property("format", gst::Format::Time);
        appsrc.set_property("is-live", true);
        appsrc.set_property("do-timestamp", true);

        // Input caps for RTP
        let caps = match codec {
            VideoCodec::H264 => gst::Caps::builder("application/x-rtp")
                .field("media", "video")
                .field("encoding-name", "H264")
                .field("payload", 96i32)
                .field("clock-rate", clock_rate as i32)
                .build(),
            VideoCodec::H265 => gst::Caps::builder("application/x-rtp")
                .field("media", "video")
                .field("encoding-name", "H265")
                .field("payload", 96i32)
                .field("clock-rate", clock_rate as i32)
                .build(),
        };
        appsrc.set_caps(Some(&caps));

        // RTP depayloader
        let depay_name = match codec {
            VideoCodec::H264 => "rtph264depay",
            VideoCodec::H265 => "rtph265depay",
        };
        let depay = gst::ElementFactory::make(depay_name)
            .build()
            .context(format!("Failed to create {}", depay_name))?;

        // Parser - ensures proper framing and extracts codec data
        // config-interval=-1 ensures VPS/SPS/PPS are inserted before each keyframe
        let parser = match codec {
            VideoCodec::H264 => gst::ElementFactory::make("h264parse")
                .property("config-interval", -1i32)
                .build()
                .context("Failed to create h264parse")?,
            VideoCodec::H265 => gst::ElementFactory::make("h265parse")
                .property("config-interval", -1i32)
                .build()
                .context("Failed to create h265parse")?,
        };

        // Muxer - use isofmp4mux for fragmented MP4 output
        // fragment-duration ensures each fragment is self-contained
        let mux = gst::ElementFactory::make("isofmp4mux")
            .property("fragment-duration", gst::ClockTime::from_seconds(1))
            .build()
            .context("Failed to create isofmp4mux - make sure gst-plugins-rs is installed")?;

        // File sink
        let filesink = gst::ElementFactory::make("filesink")
            .property("location", output_path_str)
            .property("sync", false)
            .build()
            .context("Failed to create filesink")?;

        // Add elements to pipeline
        pipeline
            .add_many([appsrc.upcast_ref(), &depay, &parser, &mux, &filesink])
            .context("Failed to add elements to pipeline")?;

        // Link elements
        gst::Element::link_many([appsrc.upcast_ref(), &depay, &parser, &mux, &filesink])
            .context("Failed to link pipeline elements")?;

        // Set up bus watch for errors
        let bus = pipeline.bus().ok_or_else(|| anyhow!("No pipeline bus"))?;
        let bus_watch = bus
            .add_watch(|_, msg| {
                match msg.view() {
                    gst::MessageView::Error(err) => {
                        error!(
                            "GStreamer error from {:?}: {} ({:?})",
                            err.src().map(|s| s.path_string()),
                            err.error(),
                            err.debug()
                        );
                    }
                    gst::MessageView::Warning(warn) => {
                        warn!(
                            "GStreamer warning from {:?}: {} ({:?})",
                            warn.src().map(|s| s.path_string()),
                            warn.error(),
                            warn.debug()
                        );
                    }
                    gst::MessageView::Eos(_) => {
                        info!("End of stream");
                    }
                    _ => {}
                }
                gst::glib::ControlFlow::Continue
            })
            .expect("Failed to add bus watch");

        // Start the pipeline
        pipeline
            .set_state(gst::State::Playing)
            .context("Failed to start pipeline")?;

        info!(
            "Recording pipeline started, writing {:?} to: {}",
            codec, output_path_str
        );

        Ok(Self {
            pipeline,
            appsrc,
            packet_count: 0,
            start_time: None,
            _bus_watch: bus_watch,
        })
    }

    /// Push a raw RTP packet to the pipeline
    fn push_rtp_packet(&mut self, data: &[u8]) -> Result<()> {
        if self.start_time.is_none() {
            self.start_time = Some(std::time::Instant::now());
            info!("First RTP packet received");
        }

        let buffer = gst::Buffer::from_slice(data.to_vec());

        match self.appsrc.push_buffer(buffer) {
            Ok(_) => {}
            Err(gst::FlowError::Eos) => {
                info!("Pipeline reached EOS");
                return Ok(());
            }
            Err(gst::FlowError::Flushing) => {
                info!("Pipeline is flushing");
                return Ok(());
            }
            Err(e) => {
                return Err(anyhow!("Failed to push buffer: {:?}", e));
            }
        }

        self.packet_count += 1;

        if self.packet_count % 500 == 0 {
            let elapsed = self.start_time.map(|t| t.elapsed()).unwrap_or_default();
            info!(
                "Pushed {} RTP packets ({:.1}s)",
                self.packet_count,
                elapsed.as_secs_f64()
            );
        }

        Ok(())
    }

    fn finish(self) -> Result<()> {
        info!("Finishing recording ({} packets)...", self.packet_count);

        // Send EOS to signal end of stream
        if let Err(e) = self.appsrc.end_of_stream() {
            warn!("Failed to send EOS to appsrc: {:?}", e);
        }

        // Wait for EOS to propagate through the pipeline
        let bus = self.pipeline.bus().ok_or_else(|| anyhow!("No bus"))?;
        let timeout = gst::ClockTime::from_seconds(10);

        info!("Waiting for pipeline to finish...");
        loop {
            match bus.timed_pop(timeout) {
                Some(msg) => match msg.view() {
                    gst::MessageView::Eos(_) => {
                        info!("EOS received, file finalized");
                        break;
                    }
                    gst::MessageView::Error(err) => {
                        error!(
                            "Error during finalization: {} ({:?})",
                            err.error(),
                            err.debug()
                        );
                        break;
                    }
                    _ => {}
                },
                None => {
                    warn!("Timeout waiting for EOS");
                    break;
                }
            }
        }

        // Stop the pipeline
        info!("Stopping pipeline...");
        if let Err(e) = self.pipeline.set_state(gst::State::Null) {
            error!("Failed to stop pipeline: {:?}", e);
        }

        let elapsed = self.start_time.map(|t| t.elapsed()).unwrap_or_default();
        info!(
            "Recording complete: {} packets in {:.1}s",
            self.packet_count,
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

async fn run() -> Result<()> {
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
            .user_agent("Retina gst-record example".to_owned())
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

    let clock_rate = session.streams()[video_stream_i].clock_rate_hz();
    info!(
        "Found {:?} video stream at index {}, clock_rate={}",
        codec, video_stream_i, clock_rate
    );

    // Setup and play
    session
        .setup(
            video_stream_i,
            SetupOptions::default().transport(opts.transport),
        )
        .await
        .context("Failed to setup stream")?;

    // Play without demuxing - we want raw RTP packets
    let mut session = session
        .play(retina::client::PlayOptions::default())
        .await
        .context("Failed to start playback")?;

    // Create recorder
    let mut recorder = Recorder::new(&opts.output, codec, clock_rate)?;

    info!("Starting to record...");

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
                    Some(Ok(retina::client::PacketItem::Rtp(rtp))) => {
                        // Only process packets from our video stream
                        if rtp.stream_id() == video_stream_i {
                            // Get raw RTP packet data (includes RTP header)
                            recorder.push_rtp_packet(rtp.raw())?;
                        }
                    }
                    Some(Ok(retina::client::PacketItem::Rtcp(_))) => {
                        // Ignore RTCP packets
                    }
                    Some(Ok(_)) => {
                        // Ignore other packet types
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
    recorder.finish()?;

    Ok(())
}
