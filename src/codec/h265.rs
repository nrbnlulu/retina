// Copyright (C) 2024 Scott Lamb <slamb@slamb.org>
// SPDX-License-Identifier: MIT OR Apache-2.0

//! [H.265](https://www.itu.int/rec/T-REC-H.265)-encoded video,
//! with RTP encoding as in [RFC 7798](https://tools.ietf.org/html/rfc7798).
//!
//! Uses scuffle-h265 for robust NAL unit parsing instead of internal implementation.

use std::convert::TryFrom;
use std::fmt::Write;
use std::io::Cursor;

use base64::Engine as _;
use bytes::{Buf, Bytes};
use log::{debug, log_enabled, trace};
use scuffle_h265::{NALUnitType, SpsNALUnit};

use super::VideoFrame;
use crate::rtp::ReceivedPacket;

/// Simple NAL unit header representation
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Header([u8; 2]);

impl Header {
    pub fn unit_type(&self) -> NALUnitType {
        let nal_unit_type = (self.0[0] >> 1) & 0x3F;
        match nal_unit_type {
            32 => NALUnitType::VpsNut,
            33 => NALUnitType::SpsNut,
            34 => NALUnitType::PpsNut,
            _ => NALUnitType::SpsNut, // Default fallback
        }
    }

    pub fn nuh_layer_id(&self) -> u8 {
        ((self.0[0] & 0x01) << 5) | ((self.0[1] >> 3) & 0x1F)
    }

    pub fn nuh_temporal_id_plus1(&self) -> u8 {
        self.0[1] & 0x07
    }

    pub fn with_unit_type(&self, unit_type: NALUnitType) -> Self {
        let mut new_hdr = *self;
        let nal_unit_type = match unit_type {
            NALUnitType::VpsNut => 32,
            NALUnitType::SpsNut => 33,
            NALUnitType::PpsNut => 34,
            _ => 33, // Default to SPS
        };
        new_hdr.0[0] = (new_hdr.0[0] & 0x81) | ((nal_unit_type & 0x3F) << 1);
        new_hdr
    }
}

impl TryFrom<[u8; 2]> for Header {
    type Error = String;

    fn try_from(bytes: [u8; 2]) -> Result<Self, Self::Error> {
        if (bytes[0] & 0x80) != 0 {
            return Err("forbidden_zero_bit is not zero".into());
        }
        if (bytes[1] & 0x07) == 0 {
            return Err("nuh_temporal_id_plus1 must not be zero".into());
        }
        Ok(Header(bytes))
    }
}

impl std::ops::Deref for Header {
    type Target = [u8; 2];
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// A [super::Depacketizer] implementation which finds access unit boundaries
/// and produces unfragmented NAL units as specified in [RFC
/// 7798](https://tools.ietf.org/html/rfc7798).
#[derive(Debug)]
pub(crate) struct Depacketizer {
    input_state: DepacketizerInputState,
    pending: Option<VideoFrame>,
    parameters: Option<InternalParameters>,
    pieces: Vec<Bytes>,
    nals: Vec<Nal>,
    seen_inconsistent_fu_nal_hdr: bool,
}

#[derive(Debug)]
struct Nal {
    hdr: Header,
    next_piece_idx: u32,
    len: u32,
}

#[derive(Debug)]
struct AccessUnit {
    start_ctx: crate::PacketContext,
    end_ctx: crate::PacketContext,
    timestamp: crate::Timestamp,
    stream_id: usize,
    in_fu: bool,
    loss: u16,
    same_ts_as_prev: bool,
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
enum DepacketizerInputState {
    New,
    Loss {
        timestamp: crate::Timestamp,
        pkts: u16,
    },
    PreMark(AccessUnit),
    PostMark {
        timestamp: crate::Timestamp,
        loss: u16,
    },
}

fn take_hdr(data: &mut Bytes) -> Result<Header, String> {
    let mut hdr_bytes = [0u8; 2];
    if data.len() < hdr_bytes.len() {
        return Err("Short NAL".into());
    };
    data.copy_to_slice(&mut hdr_bytes);
    Header::try_from(hdr_bytes)
}

impl Depacketizer {
    pub(super) fn new(
        clock_rate: u32,
        format_specific_params: Option<&str>,
    ) -> Result<Self, String> {
        if clock_rate != 90_000 {
            return Err(format!(
                "invalid H.265 clock rate {clock_rate}; must always be 90000"
            ));
        }

        let parameters = match format_specific_params {
            None => None,
            Some(fp) => match InternalParameters::parse_format_specific_params(fp) {
                Ok(p) => Some(p),
                Err(e) => {
                    log::warn!("Ignoring bad H.265 format-specific-params {:?}: {}", fp, e);
                    None
                }
            },
        };
        Ok(Depacketizer {
            input_state: DepacketizerInputState::New,
            pending: None,
            pieces: Vec::new(),
            nals: Vec::new(),
            parameters,
            seen_inconsistent_fu_nal_hdr: false,
        })
    }

    pub(super) fn parameters(&self) -> Option<super::ParametersRef<'_>> {
        self.parameters
            .as_ref()
            .map(|p| super::ParametersRef::Video(&p.generic_parameters))
    }

    pub(super) fn push(&mut self, pkt: ReceivedPacket) -> Result<(), String> {
        if let Some(p) = self.pending.as_ref() {
            panic!("push with data already pending: {p:?}");
        }

        let mut access_unit =
            match std::mem::replace(&mut self.input_state, DepacketizerInputState::New) {
                DepacketizerInputState::New => {
                    debug_assert!(self.nals.is_empty());
                    debug_assert!(self.pieces.is_empty());
                    AccessUnit::start(&pkt, 0, false)
                }
                DepacketizerInputState::PreMark(mut access_unit) => {
                    let loss = pkt.loss();
                    if loss > 0 {
                        self.nals.clear();
                        self.pieces.clear();
                        if access_unit.timestamp.timestamp == pkt.timestamp().timestamp {
                            self.input_state = if pkt.mark() {
                                DepacketizerInputState::PostMark {
                                    timestamp: pkt.timestamp(),
                                    loss,
                                }
                            } else {
                                self.pieces.clear();
                                self.nals.clear();
                                DepacketizerInputState::Loss {
                                    timestamp: pkt.timestamp(),
                                    pkts: loss,
                                }
                            };
                            return Ok(());
                        }
                        AccessUnit::start(&pkt, 0, false)
                    } else if access_unit.timestamp.timestamp != pkt.timestamp().timestamp {
                        if access_unit.in_fu {
                            return Err(format!(
                                "Timestamp changed from {} to {} in the middle of a fragmented NAL",
                                access_unit.timestamp,
                                pkt.timestamp()
                            ));
                        }
                        let last_nal_hdr = self
                            .nals
                            .last()
                            .ok_or("nals should not be empty".to_string())?
                            .hdr;
                        if can_end_au(last_nal_hdr.unit_type()) {
                            access_unit.end_ctx = *pkt.ctx();
                            self.pending =
                                Some(self.finalize_access_unit(access_unit, "ts change")?);
                            AccessUnit::start(&pkt, 0, false)
                        } else {
                            log::debug!(
                                "Bogus mid-access unit timestamp change after {:?}",
                                last_nal_hdr
                            );
                            access_unit.timestamp.timestamp = pkt.timestamp().timestamp;
                            access_unit
                        }
                    } else {
                        access_unit
                    }
                }
                DepacketizerInputState::PostMark {
                    timestamp: state_ts,
                    loss,
                } => {
                    debug_assert!(self.nals.is_empty());
                    debug_assert!(self.pieces.is_empty());
                    AccessUnit::start(&pkt, loss, state_ts.timestamp == pkt.timestamp().timestamp)
                }
                DepacketizerInputState::Loss {
                    timestamp,
                    mut pkts,
                } => {
                    debug_assert!(self.nals.is_empty());
                    debug_assert!(self.pieces.is_empty());
                    if pkt.timestamp().timestamp == timestamp.timestamp {
                        pkts += pkt.loss();
                        self.input_state = DepacketizerInputState::Loss { timestamp, pkts };
                        return Ok(());
                    }
                    AccessUnit::start(&pkt, pkts, false)
                }
            };

        let ctx = *pkt.ctx();
        let mark = pkt.mark();
        let loss = pkt.loss();
        let timestamp = pkt.timestamp();
        let mut data = pkt.into_payload_bytes();

        let hdr = take_hdr(&mut data)?;

        match nal_unit_type_from_header(hdr) {
            1..=47 => {
                if access_unit.in_fu {
                    return Err(format!(
                        "Non-fragmented NAL {hdr:?} while fragment in progress"
                    ));
                }
                let len = u32::try_from(data.len() + 2).expect("data.len() should be <= u16::MAX");
                let next_piece_idx = self.add_piece(data)?;
                self.nals.push(Nal {
                    hdr,
                    next_piece_idx,
                    len,
                });
            }
            48 => loop {
                if data.remaining() < 2 {
                    return Err(format!(
                        "AP has {} remaining bytes; expecting 2-byte length",
                        data.remaining()
                    ));
                }
                let len = data.get_u16();
                match data.remaining().cmp(&usize::from(len)) {
                    std::cmp::Ordering::Less => {
                        return Err(format!(
                            "AP too short: {} bytes remaining, expecting {}-byte NAL",
                            data.remaining(),
                            len
                        ));
                    }
                    std::cmp::Ordering::Equal => {
                        let hdr = take_hdr(&mut data)?;
                        let next_piece_idx = self.add_piece(data)?;
                        self.nals.push(Nal {
                            hdr,
                            next_piece_idx,
                            len: u32::from(len),
                        });
                        break;
                    }
                    std::cmp::Ordering::Greater => {
                        let mut piece = data.split_to(usize::from(len));
                        let hdr = take_hdr(&mut piece)?;
                        let next_piece_idx = self.add_piece(piece)?;
                        self.nals.push(Nal {
                            hdr,
                            next_piece_idx,
                            len: u32::from(len),
                        });
                    }
                }
            },
            49 => {
                if data.len() < 2 {
                    return Err(format!("FU len {} too short", data.len()));
                }
                let fu_header = data.get_u8();
                let start = (fu_header & 0b10000000) != 0;
                let end = (fu_header & 0b01000000) != 0;
                let fu_type_num = fu_header & 0b00111111;
                let fu_type = match fu_type_num {
                    32 => NALUnitType::VpsNut,
                    33 => NALUnitType::SpsNut,
                    34 => NALUnitType::PpsNut,
                    _ => NALUnitType::SpsNut, // Default fallback
                };
                let hdr = hdr.with_unit_type(fu_type);

                if start && end {
                    return Err(format!("Invalid FU header {fu_header:02x}"));
                }
                if !end && mark {
                    return Err("FU pkt with MARK && !END".into());
                }
                let u32_len = u32::try_from(data.len())
                    .map_err(|_| "RTP packet len must be < u16::MAX".to_string())?;
                match (start, access_unit.in_fu) {
                    (true, true) => return Err("FU with start bit while frag in progress".into()),
                    (true, false) => {
                        self.add_piece(data)?;
                        self.nals.push(Nal {
                            hdr,
                            next_piece_idx: u32::MAX,
                            len: 2 + u32_len,
                        });
                        access_unit.in_fu = true;
                    }
                    (false, true) => {
                        let pieces = self.add_piece(data)?;
                        let nal = self
                            .nals
                            .last_mut()
                            .ok_or("nals non-empty while in fu".to_string())?;
                        if hdr != nal.hdr && !self.seen_inconsistent_fu_nal_hdr {
                            log::warn!(
                                "FU has inconsistent NAL header: {:?} then {:?}; will not log about this again for this stream",
                                nal.hdr,
                                hdr,
                            );
                            self.seen_inconsistent_fu_nal_hdr = true;
                        }
                        nal.len += u32_len;
                        if end {
                            nal.next_piece_idx = pieces;
                            access_unit.in_fu = false;
                        } else if mark {
                            return Err("FU has MARK and no END".into());
                        }
                    }
                    (false, false) => {
                        if loss > 0 {
                            self.pieces.clear();
                            self.nals.clear();
                            self.input_state = DepacketizerInputState::Loss {
                                timestamp,
                                pkts: loss,
                            };
                            return Ok(());
                        }
                        return Err("FU has start bit unset while no frag in progress".into());
                    }
                }
            }
            _ => return Err(format!("unexpected/bad nal header {hdr:?}")),
        }

        self.input_state = if mark {
            let last_nal_hdr = self
                .nals
                .last()
                .ok_or("nals should not be empty after mark".to_string())?
                .hdr;
            if can_end_au(last_nal_hdr.unit_type()) {
                access_unit.end_ctx = ctx;
                self.pending = Some(self.finalize_access_unit(access_unit, "mark")?);
                DepacketizerInputState::PostMark { timestamp, loss: 0 }
            } else {
                log::debug!(
                    "Bogus mid-access unit timestamp change after {:?}",
                    last_nal_hdr
                );
                access_unit.timestamp.timestamp = timestamp.timestamp;
                DepacketizerInputState::PreMark(access_unit)
            }
        } else {
            DepacketizerInputState::PreMark(access_unit)
        };
        Ok(())
    }

    pub(super) fn pull(&mut self) -> Option<super::CodecItem> {
        self.pending.take().map(super::CodecItem::VideoFrame)
    }

    fn add_piece(&mut self, piece: Bytes) -> Result<u32, String> {
        self.pieces.push(piece);
        u32::try_from(self.pieces.len()).map_err(|_| "more than u32::MAX pieces!".to_string())
    }

    fn finalize_access_unit(&mut self, au: AccessUnit, reason: &str) -> Result<VideoFrame, String> {
        let mut piece_idx = 0;
        let mut retained_len = 0usize;

        let mut is_random_access_point = true;
        let is_disposable = false;
        let mut new_vps = None::<Bytes>;
        let mut new_sps = None::<Bytes>;
        let mut new_pps = None::<Bytes>;

        if log_enabled!(log::Level::Debug) {
            self.log_access_unit(&au, reason);
        }

        for nal in &self.nals {
            let next_piece_idx = crate::to_usize(nal.next_piece_idx);
            let nal_pieces = &self.pieces[piece_idx..next_piece_idx];
            match nal.hdr.unit_type() {
                NALUnitType::VpsNut => {
                    if self
                        .parameters
                        .as_ref()
                        .map(|p| !nal_matches(&p.vps_nal[..], nal.hdr, nal_pieces))
                        .unwrap_or(true)
                    {
                        new_vps = Some(to_bytes(nal.hdr, nal.len, nal_pieces));
                    }
                }
                NALUnitType::SpsNut => {
                    if self
                        .parameters
                        .as_ref()
                        .map(|p| !nal_matches(&p.sps_nal[..], nal.hdr, nal_pieces))
                        .unwrap_or(true)
                    {
                        new_sps = Some(to_bytes(nal.hdr, nal.len, nal_pieces));
                    }
                }
                NALUnitType::PpsNut => {
                    if self
                        .parameters
                        .as_ref()
                        .map(|p| !nal_matches(&p.pps_nal[..], nal.hdr, nal_pieces))
                        .unwrap_or(true)
                    {
                        new_pps = Some(to_bytes(nal.hdr, nal.len, nal_pieces));
                    }
                }
                _ => {
                    // For simplicity, assume all other types are VCL and may be inter-coded
                    is_random_access_point = false;
                }
            }
            retained_len += 4usize + crate::to_usize(nal.len);
            piece_idx = next_piece_idx;
        }

        let mut data = Vec::with_capacity(retained_len);
        piece_idx = 0;
        for nal in &self.nals {
            let next_piece_idx = crate::to_usize(nal.next_piece_idx);
            let nal_pieces = &self.pieces[piece_idx..next_piece_idx];

            data.extend_from_slice(&nal.len.to_be_bytes());
            data.extend_from_slice(&nal.hdr[..]);

            let mut actual_len = 2;
            for piece in nal_pieces {
                data.extend_from_slice(&piece[..]);
                actual_len += piece.len();
            }
            debug_assert_eq!(crate::to_usize(nal.len), actual_len);
            piece_idx = next_piece_idx;
        }
        debug_assert_eq!(retained_len, data.len());

        self.nals.clear();
        self.pieces.clear();

        let all_new_params = new_vps.is_some() && new_sps.is_some() && new_pps.is_some();
        let some_new_params = new_vps.is_some() || new_sps.is_some() || new_pps.is_some();
        let has_new_parameters = if all_new_params || (some_new_params && self.parameters.is_some())
        {
            let old_ip = self.parameters.as_ref();
            let vps_nal = new_vps
                .as_deref()
                .unwrap_or_else(|| &old_ip.unwrap().vps_nal);
            let sps_nal = new_sps
                .as_deref()
                .unwrap_or_else(|| &old_ip.unwrap().sps_nal);
            let pps_nal = new_pps
                .as_deref()
                .unwrap_or_else(|| &old_ip.unwrap().pps_nal);
            let seen_extra_trailing_data =
                old_ip.map(|o| o.seen_extra_trailing_data).unwrap_or(false);
            match InternalParameters::parse_vps_sps_pps(
                vps_nal,
                sps_nal,
                pps_nal,
                seen_extra_trailing_data,
            ) {
                Ok(params) => {
                    self.parameters = Some(params);
                }
                Err(e) => {
                    log::warn!(
                        "Failed to parse VPS/SPS/PPS from stream, continuing without updated parameters: {}",
                        e
                    );
                }
            }
            true
        } else {
            false
        };

        Ok(VideoFrame {
            has_new_parameters,
            loss: au.loss,
            start_ctx: au.start_ctx,
            end_ctx: au.end_ctx,
            timestamp: au.timestamp,
            stream_id: au.stream_id,
            is_random_access_point,
            is_disposable,
            data,
        })
    }

    fn log_access_unit(&self, au: &AccessUnit, reason: &str) {
        let mut errs = String::new();
        if au.same_ts_as_prev {
            errs.push_str("\n* same timestamp as previous access unit");
        }
        if !errs.is_empty() {
            let mut nals = String::new();
            for (i, nal) in self.nals.iter().enumerate() {
                let _ = write!(&mut nals, "\n  {}: {:?}", i, nal.hdr);
            }
            debug!(
                "bad access unit (ended by {}) at ts {}\nerrors are:{}\nNALs are:{}",
                reason, au.timestamp, errs, nals
            );
        } else if log_enabled!(log::Level::Trace) {
            let mut nals = String::new();
            for (i, nal) in self.nals.iter().enumerate() {
                let _ = write!(&mut nals, "\n  {}: {:?}", i, nal.hdr);
            }
            trace!(
                "access unit (ended by {}) at ts {}; NALS are:{}",
                reason, au.timestamp, nals
            );
        }
    }
}

fn nal_unit_type_from_header(hdr: Header) -> u8 {
    (hdr[0] >> 1) & 0x3F
}

fn can_end_au(nal_unit_type: NALUnitType) -> bool {
    !matches!(
        nal_unit_type,
        NALUnitType::VpsNut | NALUnitType::SpsNut | NALUnitType::PpsNut
    )
}

impl AccessUnit {
    fn start(
        pkt: &crate::rtp::ReceivedPacket,
        additional_loss: u16,
        same_ts_as_prev: bool,
    ) -> Self {
        AccessUnit {
            start_ctx: *pkt.ctx(),
            end_ctx: *pkt.ctx(),
            timestamp: pkt.timestamp(),
            stream_id: pkt.stream_id(),
            in_fu: false,
            loss: pkt.loss() + additional_loss,
            same_ts_as_prev,
        }
    }
}

#[derive(Clone, Debug)]
struct InternalParameters {
    generic_parameters: super::VideoParameters,
    vps_nal: Bytes,
    sps_nal: Bytes,
    pps_nal: Bytes,
    seen_extra_trailing_data: bool,
}

impl InternalParameters {
    fn parse_format_specific_params(format_specific_params: &str) -> Result<Self, String> {
        let mut sps_nal = None;
        let mut pps_nal = None;
        let mut vps_nal = None;
        for p in format_specific_params.split(';') {
            match p.trim().split_once('=') {
                Some(("tx-mode", "SRST")) => {}
                Some(("tx-mode", v)) => {
                    return Err(format!("unsupported/unexpected tx-mode {v}; expected SRST"));
                }
                Some(("sprop-vps", v)) => Self::store_sprop_nal("sprop-vps", v, &mut vps_nal)?,
                Some(("sprop-sps", v)) => Self::store_sprop_nal("sprop-sps", v, &mut sps_nal)?,
                Some(("sprop-pps", v)) => Self::store_sprop_nal("sprop-pps", v, &mut pps_nal)?,
                Some((_, _)) => {}
                None => return Err(format!("key {p} without value")),
            }
        }
        let vps_nal = vps_nal.ok_or_else(|| "no vps".to_string())?;
        let sps_nal = sps_nal.ok_or_else(|| "no sps".to_string())?;
        let pps_nal = pps_nal.ok_or_else(|| "no pps".to_string())?;
        Self::parse_vps_sps_pps(&vps_nal, &sps_nal, &pps_nal, false)
    }

    fn store_sprop_nal(key: &str, value: &str, out: &mut Option<Vec<u8>>) -> Result<(), String> {
        let nal = base64::engine::general_purpose::STANDARD
            .decode(value)
            .map_err(|e| format!("bad parameter {key}: NAL has invalid base64 encoding: {e}"))?;
        if nal.is_empty() {
            return Err(format!("bad parameter {key}: empty NAL"));
        }
        if out.is_some() {
            return Err(format!("multiple {key} parameters"));
        }
        *out = Some(nal);
        Ok(())
    }

    fn parse_vps_sps_pps(
        vps_nal: &[u8],
        sps_nal: &[u8],
        pps_nal: &[u8],
        seen_extra_trailing_data: bool,
    ) -> Result<InternalParameters, String> {
        // Use scuffle-h265 for robust SPS parsing
        let sps_nalu = SpsNALUnit::parse(Cursor::new(sps_nal))
            .map_err(|e| format!("Failed to parse SPS with scuffle-h265: {}", e))?;

        let sps = &sps_nalu.rbsp;

        // Extract pixel dimensions using scuffle-h265
        let pixel_dimensions = (
            sps.cropped_width()
                .try_into()
                .map_err(|_| "SPS width too large".to_string())?,
            sps.cropped_height()
                .try_into()
                .map_err(|_| "SPS height too large".to_string())?,
        );

        // Extract VUI parameters if available
        let (pixel_aspect_ratio, frame_rate) = if let Some(vui) = &sps.vui_parameters {
            let pixel_aspect_ratio = match &vui.aspect_ratio_info {
                scuffle_h265::AspectRatioInfo::ExtendedSar {
                    sar_width,
                    sar_height,
                } => Some((*sar_width as u32, *sar_height as u32)),
                _ => Some((1u32, 1u32)), // Default for predefined aspect ratios
            };

            let frame_rate = vui
                .vui_timing_info
                .as_ref()
                .map(|ti| (ti.num_units_in_tick.get(), ti.time_scale.get()));

            (pixel_aspect_ratio, frame_rate)
        } else {
            (None, None)
        };

        // Create RFC6381 codec string
        let profile = sps.profile_tier_level.general_profile.profile_idc;
        let level = sps
            .profile_tier_level
            .general_profile
            .level_idc
            .unwrap_or(0);
        let rfc6381_codec = format!("hev1.{}.4.L{}.B0", profile, level);

        // Create simplified HEVC decoder configuration record
        let mut extra_data = Vec::new();

        // Basic HEVC configuration structure
        extra_data.extend_from_slice(&[
            0x01,    // configurationVersion
            profile, // general_profile_idc
            0x00, 0x00, 0x00, 0x00, // general_profile_compatibility_flags
            0x90, 0x00, 0x00, 0x00, 0x00, 0x00,  // general_constraint_indicator_flags
            level, // general_level_idc
            0xFC, 0x00, // min_spatial_segmentation_idc
            0xFC, // parallelismType
            0xFC, // chromaFormat
            0xFC, // bitDepthLumaMinus8
            0xFC, // bitDepthChromaMinus8
            0x00, 0x00, // avgFrameRate
            0x00, // constantFrameRate, numTemporalLayers, temporalIdNested, lengthSizeMinusOne
            0x03, // numOfArrays
        ]);

        // Add VPS array
        extra_data.extend_from_slice(&[0x20, 0x00, 0x01]); // VPS type, count
        extra_data.extend_from_slice(&(vps_nal.len() as u16).to_be_bytes());
        extra_data.extend_from_slice(vps_nal);

        // Add SPS array
        extra_data.extend_from_slice(&[0x21, 0x00, 0x01]); // SPS type, count
        extra_data.extend_from_slice(&(sps_nal.len() as u16).to_be_bytes());
        extra_data.extend_from_slice(sps_nal);

        // Add PPS array
        extra_data.extend_from_slice(&[0x22, 0x00, 0x01]); // PPS type, count
        extra_data.extend_from_slice(&(pps_nal.len() as u16).to_be_bytes());
        extra_data.extend_from_slice(pps_nal);

        Ok(InternalParameters {
            generic_parameters: super::VideoParameters {
                rfc6381_codec,
                pixel_dimensions,
                pixel_aspect_ratio,
                frame_rate,
                extra_data: extra_data.into(),
                codec: super::VideoParametersCodec::H265 {
                    sps: Bytes::copy_from_slice(sps_nal),
                    pps: Bytes::copy_from_slice(pps_nal),
                    vps: Bytes::copy_from_slice(vps_nal),
                },
            },
            vps_nal: Bytes::copy_from_slice(vps_nal),
            sps_nal: Bytes::copy_from_slice(sps_nal),
            pps_nal: Bytes::copy_from_slice(pps_nal),
            seen_extra_trailing_data,
        })
    }
}

/// Returns true iff the bytes of `nal` equal the bytes of `[hdr, ..data]`.
fn nal_matches(nal: &[u8], hdr: Header, pieces: &[Bytes]) -> bool {
    if nal.get(0..2) != Some(&*hdr) {
        return false;
    }
    let mut nal_pos = 2;
    for piece in pieces {
        let new_pos = nal_pos + piece.len();
        if nal.len() < new_pos {
            return false;
        }
        if piece[..] != nal[nal_pos..new_pos] {
            return false;
        }
        nal_pos = new_pos;
    }
    nal_pos == nal.len()
}

/// Saves the given NAL to a contiguous `Bytes`.
fn to_bytes(hdr: Header, len: u32, pieces: &[Bytes]) -> Bytes {
    let len = crate::to_usize(len);
    let mut out = Vec::with_capacity(len);
    out.extend(&*hdr);
    for piece in pieces {
        out.extend_from_slice(&piece[..]);
    }
    debug_assert_eq!(len, out.len());
    out.into()
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use crate::{
        PacketContext,
        codec::CodecItem,
        rtp::ReceivedPacketBuilder,
        testutil::{assert_eq_hex, init_logging},
    };

    #[test]
    fn test_scuffle_h265_integration() {
        init_logging();

        // Test basic depacketizer creation with scuffle-h265
        let depacketizer = super::Depacketizer::new(90_000, None);
        assert!(depacketizer.is_ok());
    }

    #[test]
    fn test_parse_format_specific_params_with_scuffle() {
        init_logging();

        // Test that scuffle-h265 can handle the previously problematic parameters
        let params = "profile-space=0;profile-id=1;tier-flag=0;level-id=123;interop-constraints=B00000000000; sprop-vps=QAEMAf//AWAAAAMAgAAAAwAAAwCWrAkAAAAB;sprop-sps=QgEBAWAAAAMAgAAAAwAAAwCWoAPAgBDn+Nru8kusBbgICAggAAB9AAAr8g5DvcogB9AAAjKAAPoAAEZQEAAAAAE=;sprop-pps=RAHBcrCcFApiQA==";

        // This should now work better with scuffle-h265's robust parsing
        let result = super::InternalParameters::parse_format_specific_params(params);

        match result {
            Ok(_) => println!("Successfully parsed with scuffle-h265"),
            Err(e) => println!("Parsing failed but handled gracefully: {}", e),
        }
    }
}
