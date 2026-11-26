use anyhow::{bail, Context, Result};
use serde::Serialize;
use std::collections::HashMap;
use std::io::Read;

const STEAM_ID64_OFFSET: u64 = 76561197960265728;

// EDemoCommands
const DEM_STOP: u32 = 0;
const DEM_FILE_HEADER: u32 = 1;
const DEM_STRING_TABLES: u32 = 6;
const DEM_PACKET: u32 = 7;
const DEM_SIGNON_PACKET: u32 = 8;
const DEM_FULL_PACKET: u32 = 13;
const DEM_IS_COMPRESSED: u32 = 64;

// Net/User message IDs inside CDemoPacket
const SVC_SERVER_INFO: u32 = 40;
const E_USER_MSG_CHAT_MSG: u32 = 314;
const E_USER_MSG_POST_MATCH_DETAILS: u32 = 316;

#[derive(Debug, Clone, Default)]
struct UserInfoEntry {
    name: String,
    steam_id: u64,
    account_id: u32,
    fakeplayer: bool,
    ishltv: bool,
}

#[derive(Debug, Clone, Default)]
struct PostMatchPlayer {
    account_id: u32,
    hero_id: u32,
    team: u8,
}

#[derive(Debug, Clone, Default)]
struct PostMatchInfo {
    match_id: u64,
    start_time: u32,
    duration_s: u32,
    game_mode: u32,
    match_mode: u32,
    players_by_account: HashMap<u32, PostMatchPlayer>,
}

#[derive(Debug, Clone)]
struct RawChatMsg {
    tick: i32,
    player_slot: i32,
    text: String,
    all_chat: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct ChatMessageRecord {
    pub match_id: u64,
    pub start_time: i64,
    pub duration_s: u32,
    pub game_mode: u32,
    pub match_mode: u32,
    pub tick: i32,
    pub game_time_s: f32,
    pub player_slot: i32,
    pub account_id: u32,
    pub steam_id: u64,
    pub player_name: String,
    pub hero_id: u32,
    pub team: u8,
    pub chat_type: &'static str,
    pub text: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct MatchExtractionResult {
    pub match_id: u64,
    pub map_name: String,
    pub build_num: i32,
    pub start_time: i64,
    pub duration_s: u32,
    pub game_mode: u32,
    pub match_mode: u32,
    pub tick_interval: f32,
    pub player_count: usize,
    pub chat_messages: Vec<ChatMessageRecord>,
}

/// Fast streaming varint reader from an `io::Read`.
#[inline]
fn read_varint_reader<R: Read>(r: &mut R) -> Result<Option<u32>> {
    let mut val: u32 = 0;
    let mut shift: u32 = 0;
    let mut byte = [0u8; 1];
    loop {
        match r.read(&mut byte)? {
            0 => {
                if shift == 0 {
                    return Ok(None);
                } else {
                    bail!("unexpected EOF while reading varint");
                }
            }
            _ => {
                let b = byte[0];
                val |= ((b & 0x7F) as u32) << shift;
                if (b & 0x80) == 0 {
                    return Ok(Some(val));
                }
                shift += 7;
                if shift >= 35 {
                    bail!("varint overflow in demo frame header");
                }
            }
        }
    }
}

/// Minimal zero-allocation protobuf wire-format scanner over a byte slice.
struct ProtoReader<'a> {
    buf: &'a [u8],
    pos: usize,
}

enum ProtoVal<'a> {
    Varint(u64),
    Fixed64(u64),
    Bytes(&'a [u8]),
    Fixed32(u32),
}

impl<'a> ProtoReader<'a> {
    #[inline]
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    #[inline]
    fn read_varint(&mut self) -> Option<u64> {
        let mut val: u64 = 0;
        let mut shift: u32 = 0;
        while self.pos < self.buf.len() {
            let b = self.buf[self.pos];
            self.pos += 1;
            val |= ((b & 0x7F) as u64) << shift;
            if (b & 0x80) == 0 {
                return Some(val);
            }
            shift += 7;
            if shift >= 64 {
                return None;
            }
        }
        None
    }

    #[inline]
    fn next_field(&mut self) -> Option<(u32, ProtoVal<'a>)> {
        if self.pos >= self.buf.len() {
            return None;
        }
        let tag = self.read_varint()?;
        let field_num = (tag >> 3) as u32;
        let wire_type = (tag & 7) as u8;
        match wire_type {
            0 => {
                let v = self.read_varint()?;
                Some((field_num, ProtoVal::Varint(v)))
            }
            1 => {
                if self.pos + 8 > self.buf.len() {
                    return None;
                }
                let v = u64::from_le_bytes(self.buf[self.pos..self.pos + 8].try_into().ok()?);
                self.pos += 8;
                Some((field_num, ProtoVal::Fixed64(v)))
            }
            2 => {
                let len = self.read_varint()? as usize;
                if self.pos + len > self.buf.len() {
                    return None;
                }
                let slice = &self.buf[self.pos..self.pos + len];
                self.pos += len;
                Some((field_num, ProtoVal::Bytes(slice)))
            }
            5 => {
                if self.pos + 4 > self.buf.len() {
                    return None;
                }
                let v = u32::from_le_bytes(self.buf[self.pos..self.pos + 4].try_into().ok()?);
                self.pos += 4;
                Some((field_num, ProtoVal::Fixed32(v)))
            }
            _ => None,
        }
    }
}

/// LSB-first Source 2 bit-stream reader with O(1) skip for uninteresting messages.
struct BitReader<'a> {
    data: &'a [u8],
    bitpos: usize,
    total_bits: usize,
}

impl<'a> BitReader<'a> {
    #[inline]
    fn new(data: &'a [u8]) -> Self {
        Self {
            data,
            bitpos: 0,
            total_bits: data.len() * 8,
        }
    }

    #[inline]
    fn bits_left(&self) -> usize {
        self.total_bits.saturating_sub(self.bitpos)
    }

    #[inline]
    fn read_bits(&mut self, mut n: usize) -> Option<u32> {
        if self.bitpos + n > self.total_bits {
            return None;
        }
        let mut val: u32 = 0;
        let mut shift: usize = 0;
        while n > 0 {
            let byte_idx = self.bitpos >> 3;
            let bit_off = self.bitpos & 7;
            let take = n.min(8 - bit_off);
            let mask = (1u32 << take) - 1;
            let chunk = ((self.data[byte_idx] as u32) >> bit_off) & mask;
            val |= chunk << shift;
            self.bitpos += take;
            shift += take;
            n -= take;
        }
        Some(val)
    }

    #[inline]
    fn read_ubitvar(&mut self) -> Option<u32> {
        let ret = self.read_bits(6)?;
        let out = match ret & 48 {
            16 => (ret & 15) | (self.read_bits(4)? << 4),
            32 => (ret & 15) | (self.read_bits(8)? << 4),
            48 => (ret & 15) | (self.read_bits(28)? << 4),
            _ => ret,
        };
        Some(out)
    }

    #[inline]
    fn read_varint32(&mut self) -> Option<u32> {
        let mut val: u32 = 0;
        let mut shift: u32 = 0;
        loop {
            let b = self.read_bits(8)?;
            val |= (b & 0x7F) << shift;
            if (b & 0x80) == 0 {
                return Some(val);
            }
            shift += 7;
            if shift >= 35 {
                return None;
            }
        }
    }

    #[inline]
    fn skip_bytes(&mut self, n: usize) -> bool {
        let bits = n.saturating_mul(8);
        if self.bitpos + bits > self.total_bits {
            self.bitpos = self.total_bits;
            return false;
        }
        self.bitpos += bits;
        true
    }

    fn read_bytes_into(&mut self, n: usize, out: &mut Vec<u8>) -> bool {
        let bits = n.saturating_mul(8);
        if self.bitpos + bits > self.total_bits {
            return false;
        }
        out.clear();
        if (self.bitpos & 7) == 0 {
            let start = self.bitpos >> 3;
            out.extend_from_slice(&self.data[start..start + n]);
            self.bitpos += bits;
            return true;
        }
        out.reserve(n);
        let bit_off = self.bitpos & 7;
        let inv_off = 8 - bit_off;
        let mut byte_idx = self.bitpos >> 3;
        for _ in 0..n {
            let lo = self.data[byte_idx] >> bit_off;
            let hi = self.data[byte_idx + 1] << inv_off;
            out.push(lo | hi);
            byte_idx += 1;
        }
        self.bitpos += bits;
        true
    }
}

fn parse_file_header(frame: &[u8], map_name: &mut String, build_num: &mut i32) {
    let mut pr = ProtoReader::new(frame);
    while let Some((f, v)) = pr.next_field() {
        match (f, v) {
            (5, ProtoVal::Bytes(b)) => *map_name = String::from_utf8_lossy(b).into_owned(),
            (13, ProtoVal::Varint(n)) => *build_num = n as i32,
            _ => {}
        }
    }
}

fn parse_string_tables(frame: &[u8], userinfo: &mut HashMap<i32, UserInfoEntry>) {
    let mut pr = ProtoReader::new(frame);
    while let Some((f, v)) = pr.next_field() {
        if f == 1 {
            if let ProtoVal::Bytes(table_bytes) = v {
                parse_single_string_table(table_bytes, userinfo);
            }
        }
    }
}

fn parse_single_string_table(table_bytes: &[u8], userinfo: &mut HashMap<i32, UserInfoEntry>) {
    let mut pr = ProtoReader::new(table_bytes);
    let mut is_userinfo = false;
    let mut items: Vec<&[u8]> = Vec::new();
    while let Some((f, v)) = pr.next_field() {
        match (f, v) {
            (1, ProtoVal::Bytes(name)) => {
                if name == b"userinfo" {
                    is_userinfo = true;
                } else {
                    return;
                }
            }
            (2, ProtoVal::Bytes(item)) => items.push(item),
            _ => {}
        }
    }
    if !is_userinfo {
        return;
    }

    for item in items {
        let mut ipr = ProtoReader::new(item);
        let mut key_str: Option<&str> = None;
        let mut data: Option<&[u8]> = None;
        while let Some((f, v)) = ipr.next_field() {
            match (f, v) {
                (1, ProtoVal::Bytes(b)) => key_str = std::str::from_utf8(b).ok(),
                (2, ProtoVal::Bytes(b)) => data = Some(b),
                _ => {}
            }
        }
        let Some(slot) = key_str.and_then(|s| s.parse::<i32>().ok()) else {
            continue;
        };
        let Some(pbytes) = data else { continue };
        if pbytes.is_empty() {
            continue;
        }
        let mut entry = UserInfoEntry::default();
        let mut xuid: u64 = 0;
        let mut steamid: u64 = 0;
        let mut ppr = ProtoReader::new(pbytes);
        while let Some((f, v)) = ppr.next_field() {
            match (f, v) {
                (1, ProtoVal::Bytes(b)) => entry.name = String::from_utf8_lossy(b).into_owned(),
                (2, ProtoVal::Fixed64(x)) => xuid = x,
                (4, ProtoVal::Fixed64(s)) => steamid = s,
                (5, ProtoVal::Varint(b)) => entry.fakeplayer = b != 0,
                (6, ProtoVal::Varint(b)) => entry.ishltv = b != 0,
                _ => {}
            }
        }
        let resolved_steam_id = if steamid != 0 { steamid } else { xuid };
        entry.steam_id = resolved_steam_id;
        entry.account_id = if resolved_steam_id >= STEAM_ID64_OFFSET {
            (resolved_steam_id - STEAM_ID64_OFFSET) as u32
        } else {
            0
        };
        if !entry.name.is_empty() || entry.steam_id != 0 {
            userinfo.insert(slot, entry);
        }
    }
}

fn parse_chat_msg(payload: &[u8], tick: i32) -> Option<RawChatMsg> {
    let mut player_slot: i32 = -1;
    let mut text = String::new();
    let mut all_chat = false;
    let mut pr = ProtoReader::new(payload);
    while let Some((f, v)) = pr.next_field() {
        match (f, v) {
            (1, ProtoVal::Varint(n)) => player_slot = n as i32,
            (2, ProtoVal::Bytes(b)) => text = String::from_utf8_lossy(b).into_owned(),
            (3, ProtoVal::Varint(b)) => all_chat = b != 0,
            _ => {}
        }
    }
    if text.is_empty() {
        return None;
    }
    Some(RawChatMsg {
        tick,
        player_slot,
        text,
        all_chat,
    })
}

fn parse_server_info(payload: &[u8], tick_interval: &mut f32) {
    let mut pr = ProtoReader::new(payload);
    while let Some((f, v)) = pr.next_field() {
        if f == 13 {
            if let ProtoVal::Fixed32(bits) = v {
                let ti = f32::from_bits(bits);
                if ti > 0.0 && ti < 1.0 {
                    *tick_interval = ti;
                }
            }
        }
    }
}

fn parse_post_match_details(payload: &[u8]) -> Option<PostMatchInfo> {
    // CCitadelUserMsg_PostMatchDetails { optional bytes match_details = 1; }
    let mut pr = ProtoReader::new(payload);
    let mut details_bytes: Option<&[u8]> = None;
    while let Some((f, v)) = pr.next_field() {
        if f == 1 {
            if let ProtoVal::Bytes(b) = v {
                details_bytes = Some(b);
                break;
            }
        }
    }
    let details_bytes = details_bytes?;

    // CMsgMatchMetaDataContents { optional MatchInfo match_info = 2; }
    let mut dpr = ProtoReader::new(details_bytes);
    let mut info_bytes: Option<&[u8]> = None;
    while let Some((f, v)) = dpr.next_field() {
        if f == 2 {
            if let ProtoVal::Bytes(b) = v {
                info_bytes = Some(b);
                break;
            }
        }
    }
    let info_bytes = info_bytes?;

    let mut info = PostMatchInfo::default();
    let mut ipr = ProtoReader::new(info_bytes);
    while let Some((f, v)) = ipr.next_field() {
        match (f, v) {
            (1, ProtoVal::Varint(n)) => info.duration_s = n as u32,
            (4, ProtoVal::Bytes(pb)) => {
                let mut p = PostMatchPlayer::default();
                let mut ppr = ProtoReader::new(pb);
                while let Some((pf, pv)) = ppr.next_field() {
                    match (pf, pv) {
                        (1, ProtoVal::Varint(a)) => p.account_id = a as u32,
                        (6, ProtoVal::Varint(t)) => p.team = t as u8,
                        (12, ProtoVal::Varint(h)) => p.hero_id = h as u32,
                        _ => {}
                    }
                }
                if p.account_id != 0 {
                    info.players_by_account.insert(p.account_id, p);
                }
            }
            (5, ProtoVal::Varint(n)) => info.start_time = n as u32,
            (6, ProtoVal::Varint(n)) => info.match_id = n,
            (9, ProtoVal::Varint(n)) => info.game_mode = n as u32,
            (10, ProtoVal::Varint(n)) => info.match_mode = n as u32,
            _ => {}
        }
    }
    Some(info)
}

fn scan_packet_data(
    packet_frame: &[u8],
    tick: i32,
    chats: &mut Vec<RawChatMsg>,
    post_match: &mut Option<PostMatchInfo>,
    tick_interval: &mut f32,
    msg_scratch: &mut Vec<u8>,
) {
    let mut pr = ProtoReader::new(packet_frame);
    while let Some((f, v)) = pr.next_field() {
        if f != 3 {
            continue;
        }
        let ProtoVal::Bytes(data) = v else { continue };
        let mut br = BitReader::new(data);
        while br.bits_left() >= 8 {
            let Some(msg_type) = br.read_ubitvar() else {
                break;
            };
            let Some(msg_size) = br.read_varint32() else {
                break;
            };
            let msg_size = msg_size as usize;
            match msg_type {
                E_USER_MSG_CHAT_MSG => {
                    if !br.read_bytes_into(msg_size, msg_scratch) {
                        break;
                    }
                    if let Some(chat) = parse_chat_msg(msg_scratch, tick) {
                        chats.push(chat);
                    }
                }
                E_USER_MSG_POST_MATCH_DETAILS => {
                    if !br.read_bytes_into(msg_size, msg_scratch) {
                        break;
                    }
                    if let Some(pm) = parse_post_match_details(msg_scratch) {
                        *post_match = Some(pm);
                    }
                }
                SVC_SERVER_INFO => {
                    if !br.read_bytes_into(msg_size, msg_scratch) {
                        break;
                    }
                    parse_server_info(msg_scratch, tick_interval);
                }
                _ => {
                    if !br.skip_bytes(msg_size) {
                        break;
                    }
                }
            }
        }
    }
}

fn parse_full_packet(
    frame: &[u8],
    tick: i32,
    userinfo: &mut HashMap<i32, UserInfoEntry>,
    chats: &mut Vec<RawChatMsg>,
    post_match: &mut Option<PostMatchInfo>,
    tick_interval: &mut f32,
    msg_scratch: &mut Vec<u8>,
) {
    let mut pr = ProtoReader::new(frame);
    while let Some((f, v)) = pr.next_field() {
        match (f, v) {
            (1, ProtoVal::Bytes(st)) => parse_string_tables(st, userinfo),
            (2, ProtoVal::Bytes(pkt)) => {
                scan_packet_data(pkt, tick, chats, post_match, tick_interval, msg_scratch)
            }
            _ => {}
        }
    }
}

/// Stream-parse a raw (already decompressed) PBDEMS2 demo stream and extract all chat messages.
/// Never decodes entity deltas (`svc_PacketEntities`), making CPU cost negligible (~20-50ms/match).
pub fn parse_demo_stream<R: Read>(
    mut reader: R,
    fallback_match_id: u64,
) -> Result<MatchExtractionResult> {
    let mut header = [0u8; 16];
    reader
        .read_exact(&mut header)
        .context("failed to read PBDEMS2 16-byte header")?;
    if &header[..8] != b"PBDEMS2\x00" {
        bail!(
            "invalid demo magic bytes: expected PBDEMS2\\0, got {:?}",
            &header[..8]
        );
    }

    let mut map_name = String::new();
    let mut build_num: i32 = 0;
    let mut tick_interval: f32 = 1.0 / 64.0;
    let mut userinfo: HashMap<i32, UserInfoEntry> = HashMap::with_capacity(20);
    let mut raw_chats: Vec<RawChatMsg> = Vec::new();
    let mut post_match: Option<PostMatchInfo> = None;

    let mut frame_buf: Vec<u8> = Vec::with_capacity(64 * 1024);
    let mut decomp_buf: Vec<u8> = Vec::with_capacity(128 * 1024);
    let mut msg_scratch: Vec<u8> = Vec::with_capacity(64 * 1024);
    let mut snap_decoder = snap::raw::Decoder::new();

    while let Some(cmd) = read_varint_reader(&mut reader)? {
        let Some(tick_u32) = read_varint_reader(&mut reader)? else {
            break;
        };
        let Some(size_u32) = read_varint_reader(&mut reader)? else {
            break;
        };
        let size = size_u32 as usize;
        let tick = tick_u32 as i32;
        let is_compressed = (cmd & DEM_IS_COMPRESSED) != 0;
        let base_cmd = cmd & !DEM_IS_COMPRESSED;

        if base_cmd == DEM_STOP {
            break;
        }

        // Guard against corrupt frame sizes (> 64 MB)
        if size > 64 * 1024 * 1024 {
            bail!("implausible frame size {} for cmd {}", size, base_cmd);
        }

        let interested = matches!(
            base_cmd,
            DEM_FILE_HEADER
                | DEM_STRING_TABLES
                | DEM_PACKET
                | DEM_SIGNON_PACKET
                | DEM_FULL_PACKET
        );

        frame_buf.resize(size, 0);
        if let Err(e) = reader.read_exact(&mut frame_buf) {
            // Gracefully handle truncated tail on incomplete demos
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                break;
            }
            return Err(e.into());
        }

        if !interested {
            continue;
        }

        let payload: &[u8] = if is_compressed {
            let decomp_len = snap::raw::decompress_len(&frame_buf)
                .context("invalid snappy frame length")?;
            decomp_buf.resize(decomp_len, 0);
            let actual = snap_decoder
                .decompress(&frame_buf, &mut decomp_buf)
                .context("snappy decompression failed")?;
            &decomp_buf[..actual]
        } else {
            &frame_buf[..size]
        };

        match base_cmd {
            DEM_FILE_HEADER => parse_file_header(payload, &mut map_name, &mut build_num),
            DEM_STRING_TABLES => parse_string_tables(payload, &mut userinfo),
            DEM_PACKET | DEM_SIGNON_PACKET => scan_packet_data(
                payload,
                tick,
                &mut raw_chats,
                &mut post_match,
                &mut tick_interval,
                &mut msg_scratch,
            ),
            DEM_FULL_PACKET => parse_full_packet(
                payload,
                tick,
                &mut userinfo,
                &mut raw_chats,
                &mut post_match,
                &mut tick_interval,
                &mut msg_scratch,
            ),
            _ => {}
        }
    }

    let pm = post_match.unwrap_or_default();
    let match_id = if pm.match_id != 0 {
        pm.match_id
    } else {
        fallback_match_id
    };

    let human_players = userinfo
        .values()
        .filter(|u| !u.ishltv && !u.fakeplayer && u.account_id != 0)
        .count();

    let chat_messages = raw_chats
        .into_iter()
        .map(|rc| {
            let ui = userinfo.get(&rc.player_slot);
            let account_id = ui.map(|u| u.account_id).unwrap_or(0);
            let steam_id = ui.map(|u| u.steam_id).unwrap_or(0);
            let player_name = ui.map(|u| u.name.clone()).unwrap_or_default();
            let pm_player = pm.players_by_account.get(&account_id);
            let hero_id = pm_player.map(|p| p.hero_id).unwrap_or(0);
            let team = pm_player.map(|p| p.team).unwrap_or(0);
            let game_time_s = if rc.tick >= 0 {
                rc.tick as f32 * tick_interval
            } else {
                0.0
            };
            ChatMessageRecord {
                match_id,
                start_time: pm.start_time as i64,
                duration_s: pm.duration_s,
                game_mode: pm.game_mode,
                match_mode: pm.match_mode,
                tick: rc.tick,
                game_time_s,
                player_slot: rc.player_slot,
                account_id,
                steam_id,
                player_name,
                hero_id,
                team,
                chat_type: if rc.all_chat { "all" } else { "team" },
                text: rc.text,
            }
        })
        .collect();

    Ok(MatchExtractionResult {
        match_id,
        map_name,
        build_num,
        start_time: pm.start_time as i64,
        duration_s: pm.duration_s,
        game_mode: pm.game_mode,
        match_mode: pm.match_mode,
        tick_interval,
        player_count: human_players,
        chat_messages,
    })
}
