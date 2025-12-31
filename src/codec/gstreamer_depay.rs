// Copyright (C) 2025
// SPDX-License-Identifier: MIT OR Apache-2.0

//! GStreamer-based RTP depacketizer for H.264 and H.265 video streams.
//!
//! This module uses GStreamer's rtph264depay/rtph265depay elements to handle
//! the complex RTP depacketization. NAL units with the same RTP timestamp
//! are aggregated into complete access units (frames).

use bytes::{BufMut, Bytes, BytesMut};
use gstreamer as gst;
use gstreamer::prelude::*;
use gstreamer_app as gst_app;
use std::collections::VecDeque;
use std::num::NonZeroU32;
use std::str::FromStr;

use crate::PacketContext;
use crate::Timestamp;
use crate::codec::{CodecItem, VideoFrame, VideoParameters, VideoParametersCodec};
use crate::rtp::ReceivedPacket;

/// A frame being assembled from multiple NAL units
#[derive(Debug)]
struct PendingFrame {
    /// RTP timestamp for this frame
    timestamp: crate::Timestamp,
    /// Accumulated NAL data (in byte-stream/Annex B format)
    data: BytesMut,
    /// Whether this frame contains a keyframe (IDR)
    is_random_access_point: bool,
    /// Stream ID from RTP
    stream_id: usize,
}

#[derive(Debug)]
pub(crate) struct Depacketizer {
    #[allow(dead_code)]
    pipeline: gst::Pipeline,
    appsrc: gst_app::AppSrc,
    appsink: gst_app::AppSink,
    clock_rate: u32,
    parameters: Option<VideoParameters>,
    codec_name: String,
    /// Queue of completed frames ready to be pulled
    completed_frames: VecDeque<VideoFrame>,
    /// Current frame being assembled from NAL units
    pending_frame: Option<PendingFrame>,
    /// Track the last stream_id we saw
    last_stream_id: usize,
    /// Whether we've signaled new parameters
    parameters_sent: bool,
}

impl Depacketizer {
    pub fn new(
        codec: &str,
        clock_rate: u32,
        _format_specific_params: Option<&str>,
    ) -> Result<Self, String> {
        gst::init().map_err(|e| e.to_string())?;

        // Build caps for RTP input
        let caps_str = format!(
            "application/x-rtp,media=video,clock-rate={},encoding-name={},payload=96",
            clock_rate,
            codec.to_uppercase()
        );

        let caps = gst::Caps::from_str(&caps_str).map_err(|e| e.to_string())?;

        // Pipeline:
        // - appsrc: receives raw RTP packets
        // - rtph264depay/rtph265depay: depacketizes RTP into NAL units (byte-stream format)
        // - appsink: we pull NAL units and aggregate them by timestamp
        //
        // We don't use h264parse/h265parse because they corrupt timestamps.
        // Instead, we aggregate NAL units ourselves based on RTP timestamp.
        let pipeline_str = match codec {
            "h264" => {
                "appsrc name=src format=time do-timestamp=false is-live=true ! \
                 rtph264depay ! \
                 appsink name=sink sync=false"
            }
            #[cfg(feature = "h265")]
            "h265" => {
                "appsrc name=src format=time do-timestamp=false is-live=true ! \
                 rtph265depay ! \
                 appsink name=sink sync=false"
            }
            _ => return Err(format!("Unsupported codec for GStreamer depay: {}", codec)),
        };

        let pipeline = gst::parse::launch(pipeline_str)
            .map_err(|e| e.to_string())?
            .downcast::<gst::Pipeline>()
            .map_err(|_| "Expected a pipeline".to_string())?;

        let appsrc = pipeline
            .by_name("src")
            .ok_or("Missing appsrc")?
            .downcast::<gst_app::AppSrc>()
            .map_err(|_| "Expected AppSrc")?;

        // Set caps on appsrc - required for rtph264depay/rtph265depay to work
        appsrc.set_caps(Some(&caps));

        let appsink = pipeline
            .by_name("sink")
            .ok_or("Missing appsink")?
            .downcast::<gst_app::AppSink>()
            .map_err(|_| "Expected AppSink")?;

        pipeline
            .set_state(gst::State::Playing)
            .map_err(|e| e.to_string())?;

        Ok(Self {
            pipeline,
            appsrc,
            appsink,
            clock_rate,
            parameters: None,
            codec_name: codec.to_string(),
            completed_frames: VecDeque::new(),
            pending_frame: None,
            last_stream_id: 0,
            parameters_sent: false,
        })
    }

    pub fn parameters(&self) -> Option<super::ParametersRef<'_>> {
        self.parameters.as_ref().map(super::ParametersRef::Video)
    }

    pub fn push(&mut self, pkt: ReceivedPacket) -> Result<(), String> {
        self.last_stream_id = pkt.stream_id();
        let rtp_timestamp = pkt.timestamp();

        // Get the raw RTP packet data (including RTP header)
        let raw_data = pkt.raw.0;

        // Create a GStreamer buffer from the raw RTP packet
        let mut buffer = gst::Buffer::from_slice(raw_data);
        {
            let buffer_ref = buffer.get_mut().ok_or("Failed to get mutable buffer")?;

            // Convert RTP timestamp to PTS in nanoseconds
            let rtp_ts_raw = rtp_timestamp.timestamp() as u64;
            let pts_ns = rtp_ts_raw
                .checked_mul(1_000_000_000)
                .and_then(|t| t.checked_div(self.clock_rate as u64))
                .unwrap_or(0);

            buffer_ref.set_pts(gst::ClockTime::from_nseconds(pts_ns));
        }

        self.appsrc
            .push_buffer(buffer)
            .map_err(|e| format!("Failed to push buffer: {}", e))?;

        // After pushing, drain any available output from GStreamer
        // Frames are flushed automatically when timestamp changes
        self.drain_output()?;

        Ok(())
    }

    /// Drain all available output from GStreamer's appsink and aggregate by timestamp
    fn drain_output(&mut self) -> Result<(), String> {
        loop {
            // Try to pull a sample without blocking
            let sample = match self.appsink.try_pull_sample(gst::ClockTime::ZERO) {
                Some(s) => s,
                None => break,
            };

            // Update parameters from caps if not yet done
            if self.parameters.is_none() {
                if let Some(caps) = sample.caps() {
                    if let Ok(Some(params)) = self.parse_caps(caps) {
                        self.parameters = Some(params);
                    }
                }
            }

            let buffer = sample.buffer().ok_or("Sample missing buffer")?;

            // Get PTS and convert back to RTP timestamp
            let pts_ns = buffer.pts().map(|p| p.nseconds()).unwrap_or(0);
            let rtp_ts_val = (pts_ns * self.clock_rate as u64) / 1_000_000_000;

            let timestamp = Timestamp::new(
                rtp_ts_val as i64,
                NonZeroU32::new(self.clock_rate).unwrap(),
                0,
            )
            .ok_or("Failed to construct timestamp")?;

            // Map the buffer to read its data
            let map = buffer.map_readable().map_err(|_| "Failed to map buffer")?;
            let nal_data = map.as_slice();

            // Check buffer flags
            let is_keyframe = !buffer.flags().contains(gst::BufferFlags::DELTA_UNIT);

            // Check if this NAL belongs to the current pending frame (same timestamp)
            if let Some(ref mut pending) = self.pending_frame {
                if pending.timestamp.timestamp() == timestamp.timestamp() {
                    // Same timestamp - append to pending frame
                    pending.data.put_slice(nal_data);
                    if is_keyframe {
                        pending.is_random_access_point = true;
                    }
                } else {
                    // Different timestamp - flush pending frame and start new one
                    let completed = self.pending_frame.take();
                    if let Some(frame) = completed {
                        self.complete_frame(frame)?;
                    }

                    // Start new pending frame
                    let mut data = BytesMut::with_capacity(nal_data.len());
                    data.put_slice(nal_data);
                    self.pending_frame = Some(PendingFrame {
                        timestamp,
                        data,
                        is_random_access_point: is_keyframe,
                        stream_id: self.last_stream_id,
                    });
                }
            } else {
                // No pending frame - start new one
                let mut data = BytesMut::with_capacity(nal_data.len());
                data.put_slice(nal_data);
                self.pending_frame = Some(PendingFrame {
                    timestamp,
                    data,
                    is_random_access_point: is_keyframe,
                    stream_id: self.last_stream_id,
                });
            }
        }

        Ok(())
    }

    /// Flush the pending frame to the completed queue
    fn flush_pending_frame(&mut self) -> Result<(), String> {
        if let Some(pending) = self.pending_frame.take() {
            self.complete_frame(pending)?;
        }
        Ok(())
    }

    /// Complete a frame and add it to the output queue
    fn complete_frame(&mut self, pending: PendingFrame) -> Result<(), String> {
        // Skip empty frames
        if pending.data.is_empty() {
            return Ok(());
        }

        // Signal new parameters on first frame after getting them
        let has_new_parameters = self.parameters.is_some() && !self.parameters_sent;
        if has_new_parameters {
            self.parameters_sent = true;
        }

        let video_frame = VideoFrame {
            start_ctx: PacketContext::dummy(),
            end_ctx: PacketContext::dummy(),
            has_new_parameters,
            loss: 0,
            timestamp: pending.timestamp,
            stream_id: pending.stream_id,
            is_random_access_point: pending.is_random_access_point,
            is_disposable: false,
            data: pending.data.freeze().to_vec(),
        };

        self.completed_frames.push_back(video_frame);
        Ok(())
    }

    pub fn pull(&mut self) -> Result<Option<CodecItem>, String> {
        // Return the next completed frame if available
        if let Some(frame) = self.completed_frames.pop_front() {
            return Ok(Some(CodecItem::VideoFrame(frame)));
        }

        Ok(None)
    }

    fn parse_caps(&self, caps: &gst::CapsRef) -> Result<Option<VideoParameters>, String> {
        let s = caps.structure(0).ok_or("Caps has no structure")?;

        let width = s.get::<i32>("width").unwrap_or(0) as u16;
        let height = s.get::<i32>("height").unwrap_or(0) as u16;

        // Pixel Aspect Ratio
        let pixel_aspect_ratio = s
            .get::<gst::Fraction>("pixel-aspect-ratio")
            .ok()
            .map(|par| (par.numer() as u32, par.denom() as u32));

        // Frame Rate
        let frame_rate = s
            .get::<gst::Fraction>("framerate")
            .ok()
            .map(|fps| (fps.numer() as u32, fps.denom() as u32));

        // In byte-stream format, there's typically no codec_data - SPS/PPS are in the stream
        let (extra_data, codec_param) =
            if let Ok(extra_data_buf) = s.get::<gst::Buffer>("codec_data") {
                let map = extra_data_buf
                    .map_readable()
                    .map_err(|_| "Failed to map codec_data")?;
                let extra_data = Bytes::copy_from_slice(map.as_slice());

                let codec_param = match self.codec_name.as_str() {
                    "h264" => {
                        if let Some((sps, pps)) = parse_avcc(&extra_data) {
                            VideoParametersCodec::H264 { sps, pps }
                        } else {
                            VideoParametersCodec::H264 {
                                sps: Bytes::new(),
                                pps: Bytes::new(),
                            }
                        }
                    }
                    #[cfg(feature = "h265")]
                    "h265" => VideoParametersCodec::H265 {
                        vps: Bytes::new(),
                        sps: Bytes::new(),
                        pps: Bytes::new(),
                    },
                    _ => return Ok(None),
                };
                (extra_data, codec_param)
            } else {
                // byte-stream format: no codec_data, parameters are in-band
                let codec_param = match self.codec_name.as_str() {
                    "h264" => VideoParametersCodec::H264 {
                        sps: Bytes::new(),
                        pps: Bytes::new(),
                    },
                    #[cfg(feature = "h265")]
                    "h265" => VideoParametersCodec::H265 {
                        vps: Bytes::new(),
                        sps: Bytes::new(),
                        pps: Bytes::new(),
                    },
                    _ => return Ok(None),
                };
                (Bytes::new(), codec_param)
            };

        // Generate appropriate rfc6381 codec string
        let rfc6381_codec = match self.codec_name.as_str() {
            "h264" => "avc1.4D401E".to_string(), // TODO: extract profile/level from SPS
            "h265" => "hev1.1.6.L93.B0".to_string(), // TODO: extract from VPS/SPS
            _ => "unknown".to_string(),
        };

        Ok(Some(VideoParameters {
            pixel_dimensions: (width, height),
            rfc6381_codec,
            pixel_aspect_ratio,
            frame_rate,
            extra_data,
            codec: codec_param,
        }))
    }
}

/// Parse AVCC format codec_data to extract SPS and PPS
fn parse_avcc(data: &[u8]) -> Option<(Bytes, Bytes)> {
    if data.len() < 7 {
        return None;
    }
    let num_sps = data[5] & 0x1F;
    let mut pos = 6;
    let mut sps = None;
    for _ in 0..num_sps {
        if pos + 2 > data.len() {
            return None;
        }
        let len = u16::from_be_bytes([data[pos], data[pos + 1]]) as usize;
        pos += 2;
        if pos + len > data.len() {
            return None;
        }
        if sps.is_none() {
            sps = Some(Bytes::copy_from_slice(&data[pos..pos + len]));
        }
        pos += len;
    }

    if pos >= data.len() {
        return None;
    }

    let num_pps = data[pos];
    pos += 1;
    let mut pps = None;
    for _ in 0..num_pps {
        if pos + 2 > data.len() {
            return None;
        }
        let len = u16::from_be_bytes([data[pos], data[pos + 1]]) as usize;
        pos += 2;
        if pos + len > data.len() {
            return None;
        }
        if pps.is_none() {
            pps = Some(Bytes::copy_from_slice(&data[pos..pos + len]));
        }
        pos += len;
    }
    Some((sps?, pps?))
}
