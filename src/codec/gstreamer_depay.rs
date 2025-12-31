// Copyright (C) 2025
// SPDX-License-Identifier: MIT OR Apache-2.0

use bytes::Bytes;
use gstreamer as gst;
use gstreamer::prelude::*;
use gstreamer_app as gst_app;
use std::num::NonZeroU32;
use std::str::FromStr;

use crate::PacketContext;
use crate::Timestamp;
use crate::codec::{CodecItem, VideoFrame, VideoParameters, VideoParametersCodec};
use crate::rtp::ReceivedPacket;

#[derive(Debug)]
pub(crate) struct Depacketizer {
    #[allow(dead_code)]
    pipeline: gst::Pipeline,
    appsrc: gst_app::AppSrc,
    appsink: gst_app::AppSink,
    clock_rate: u32,
    parameters: Option<VideoParameters>,
    codec_name: String,
    last_stream_id: usize,
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

        let pipeline_str = match codec {
            "h264" => {
                "appsrc name=src format=time do-timestamp=false is-live=true ! \
                 rtph264depay ! \
                 h264parse config-interval=-1 ! \
                 video/x-h264,stream-format=byte-stream,alignment=au ! \
                 appsink name=sink sync=false drop=false max-buffers=100 emit-signals=false"
            }
            #[cfg(feature = "h265")]
            "h265" => {
                "appsrc name=src format=time do-timestamp=false is-live=true ! \
                 rtph265depay ! \
                 h265parse config-interval=-1 ! \
                 video/x-h265,stream-format=byte-stream,alignment=au ! \
                 appsink name=sink sync=false drop=false max-buffers=100 emit-signals=false"
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

        // Set caps on appsrc - this is crucial for rtph264depay to work
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
            last_stream_id: 0,
        })
    }

    pub fn parameters(&self) -> Option<super::ParametersRef<'_>> {
        self.parameters.as_ref().map(super::ParametersRef::Video)
    }

    pub fn push(&mut self, pkt: ReceivedPacket) -> Result<(), String> {
        self.last_stream_id = pkt.stream_id();
        let timestamp = pkt.timestamp();

        // Use the full raw packet (header + payload) as required by rtph264depay/rtph265depay
        let raw_data = pkt.raw.0;

        let mut buffer = gst::Buffer::from_slice(raw_data);
        {
            let buffer_ref = buffer.get_mut().ok_or("Failed to get mutable buffer")?;

            // Convert RTP timestamp to PTS in nanoseconds
            let pts_ns = if timestamp.timestamp() >= 0 {
                (timestamp.timestamp() as u64)
                    .checked_mul(1_000_000_000)
                    .map(|t| t / self.clock_rate as u64)
                    .unwrap_or(0)
            } else {
                0
            };

            buffer_ref.set_pts(gst::ClockTime::from_nseconds(pts_ns));
            buffer_ref.set_dts(gst::ClockTime::from_nseconds(pts_ns));
        }

        match self.appsrc.push_buffer(buffer) {
            Ok(_) => Ok(()),
            Err(e) => Err(format!("Failed to push buffer: {}", e)),
        }
    }

    pub fn pull(&mut self) -> Result<Option<CodecItem>, String> {
        let sample = match self.appsink.try_pull_sample(gst::ClockTime::ZERO) {
            Some(s) => s,
            None => return Ok(None),
        };

        let mut new_params = false;
        if let Some(caps) = sample.caps() {
            if self.parameters.is_none() {
                if let Ok(Some(params)) = self.parse_caps(caps) {
                    self.parameters = Some(params);
                    new_params = true;
                }
            }
        }

        let buffer = sample.buffer().ok_or("Sample missing buffer")?;
        let pts = buffer.pts().unwrap_or(gst::ClockTime::ZERO).nseconds();

        // Recover RTP timestamp from PTS
        let rtp_ts_val = (pts * self.clock_rate as u64) / 1_000_000_000;
        let timestamp = Timestamp::new(
            rtp_ts_val as i64,
            NonZeroU32::new(self.clock_rate).unwrap(),
            0,
        )
        .ok_or("Failed to construct timestamp")?;

        let map = buffer.map_readable().map_err(|_| "Failed to map buffer")?;
        let data = map.as_slice().to_vec();

        let is_random_access_point = !buffer.flags().contains(gst::BufferFlags::DELTA_UNIT);
        let is_disposable = buffer.flags().contains(gst::BufferFlags::DROPPABLE);

        Ok(Some(CodecItem::VideoFrame(VideoFrame {
            start_ctx: PacketContext::dummy(),
            end_ctx: PacketContext::dummy(),
            has_new_parameters: new_params,
            loss: 0,
            timestamp,
            stream_id: self.last_stream_id,
            is_random_access_point,
            is_disposable,
            data,
        })))
    }

    fn parse_caps(&self, caps: &gst::CapsRef) -> Result<Option<VideoParameters>, String> {
        let s = caps.structure(0).ok_or("Caps has no structure")?;

        let width = s.get::<i32>("width").unwrap_or(0) as u16;
        let height = s.get::<i32>("height").unwrap_or(0) as u16;

        // Pixel Aspect Ratio
        let pixel_aspect_ratio = if let Ok(par) = s.get::<gst::Fraction>("pixel-aspect-ratio") {
            Some((par.numer() as u32, par.denom() as u32))
        } else {
            None
        };

        // Frame Rate
        let frame_rate = if let Ok(fps) = s.get::<gst::Fraction>("framerate") {
            Some((fps.numer() as u32, fps.denom() as u32))
        } else {
            None
        };

        // In byte-stream format, there's no codec_data - SPS/PPS are in the stream itself.
        // We create minimal parameters without extra_data in this case.
        let (extra_data, codec_param) =
            if let Ok(extra_data_buf) = s.get::<gst::Buffer>("codec_data") {
                let map = extra_data_buf
                    .map_readable()
                    .map_err(|_| "Failed to map codec_data")?;
                let extra_data = Bytes::copy_from_slice(map.as_slice());

                let codec_param = match self.codec_name.as_str() {
                    "h264" => {
                        let (sps, pps) = parse_avcc(&extra_data).ok_or("Failed to parse AVCC")?;
                        VideoParametersCodec::H264 { sps, pps }
                    }
                    #[cfg(feature = "h265")]
                    "h265" => {
                        // HVCC parsing not implemented; use empty VPS/SPS/PPS
                        VideoParametersCodec::H265 {
                            vps: Bytes::new(),
                            sps: Bytes::new(),
                            pps: Bytes::new(),
                        }
                    }
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

    // Safety check before reading PPS count
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
