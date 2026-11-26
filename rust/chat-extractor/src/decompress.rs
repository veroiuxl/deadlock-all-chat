use anyhow::{Context, Result};
use bzip2::read::MultiBzDecoder;
use flate2::read::GzDecoder;
use std::io::{self, BufReader, Cursor, Read};
use zstd::stream::read::Decoder as ZstdDecoder;

use crate::parser::{parse_demo_stream, MatchExtractionResult};

/// Peek the first 8 bytes of any `Read` stream and automatically wrap it in
/// the matching decompressor (Zstandard, Bzip2, Gzip, or raw PBDEMS2).
///
/// Valve's replay URLs end in `.dem.bz2` even for newer Zstandard-compressed
/// demos, so sniffing magic bytes is required rather than trusting extensions.
pub fn parse_compressed_stream<R: Read>(
    mut reader: R,
    fallback_match_id: u64,
) -> Result<MatchExtractionResult> {
    let mut magic = [0u8; 8];
    let mut filled = 0;
    while filled < magic.len() {
        let n = reader.read(&mut magic[filled..])?;
        if n == 0 {
            break;
        }
        filled += n;
    }

    let chained = Cursor::new(magic[..filled].to_vec()).chain(reader);

    if filled >= 4 && magic[..4] == [0x28, 0xB5, 0x2F, 0xFD] {
        // Zstandard
        let decoder = ZstdDecoder::new(BufReader::with_capacity(256 * 1024, chained))
            .context("failed to initialize zstd decoder")?;
        parse_demo_stream(BufReader::with_capacity(256 * 1024, decoder), fallback_match_id)
    } else if filled >= 3 && &magic[..3] == b"BZh" {
        // Bzip2 (multi-stream safe)
        let decoder = MultiBzDecoder::new(BufReader::with_capacity(256 * 1024, chained));
        parse_demo_stream(BufReader::with_capacity(256 * 1024, decoder), fallback_match_id)
    } else if filled >= 2 && magic[..2] == [0x1F, 0x8B] {
        // Gzip
        let decoder = GzDecoder::new(BufReader::with_capacity(256 * 1024, chained));
        parse_demo_stream(BufReader::with_capacity(256 * 1024, decoder), fallback_match_id)
    } else {
        // Raw PBDEMS2
        parse_demo_stream(BufReader::with_capacity(256 * 1024, chained), fallback_match_id)
    }
}

/// Bridge from an async `tokio::sync::mpsc::Receiver<Result<Vec<u8>, String>>`
/// to a synchronous `std::io::Read` so HTTP response chunks stream straight
/// into Zstd/Bzip2 decompression and PBDEMS2 parsing with zero disk I/O.
pub struct ChannelReader {
    rx: tokio::sync::mpsc::Receiver<Result<Vec<u8>, String>>,
    current: Vec<u8>,
    pos: usize,
    done: bool,
}

impl ChannelReader {
    pub fn new(rx: tokio::sync::mpsc::Receiver<Result<Vec<u8>, String>>) -> Self {
        Self {
            rx,
            current: Vec::new(),
            pos: 0,
            done: false,
        }
    }
}

impl Read for ChannelReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            if self.pos < self.current.len() {
                let avail = self.current.len() - self.pos;
                let n = avail.min(buf.len());
                buf[..n].copy_from_slice(&self.current[self.pos..self.pos + n]);
                self.pos += n;
                return Ok(n);
            }
            if self.done {
                return Ok(0);
            }
            match self.rx.blocking_recv() {
                Some(Ok(chunk)) => {
                    if chunk.is_empty() {
                        continue;
                    }
                    self.current = chunk;
                    self.pos = 0;
                }
                Some(Err(err)) => {
                    self.done = true;
                    return Err(io::Error::new(io::ErrorKind::Other, err));
                }
                None => {
                    self.done = true;
                    return Ok(0);
                }
            }
        }
    }
}
