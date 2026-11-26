mod batch;
mod decompress;
mod parser;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use std::fs::File;
use std::io::{self, Write};
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "deadlock-chat-extractor",
    version,
    about = "Ultra-fast zero-entity-decoding streaming chat extractor for Deadlock Source 2 replays"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Extract chat messages from a single replay file (.dem, .dem.bz2, .dem.zst) or '-' for stdin
    Extract {
        /// Path to replay file or '-' to read from stdin
        input: PathBuf,
        /// Optional fallback match ID if PostMatchDetails is absent
        #[arg(long, default_value_t = 0)]
        match_id: u64,
        /// Output full JSON summary instead of NDJSON chat lines
        #[arg(long)]
        summary: bool,
    },
    /// Stream-download, decompress, and extract chat messages from a batch of replay URLs (JSONL)
    Batch {
        /// Path to JSONL file of {"match_id", "demo_url", "cluster_id", "start_time"} or '-' for stdin
        #[arg(short, long)]
        input: PathBuf,
        /// Output NDJSON file for extracted chat message records (optional when --stream is set)
        #[arg(short, long)]
        output: Option<PathBuf>,
        /// Optional NDJSON file for per-match status/checkpoint records
        #[arg(short, long)]
        status: Option<PathBuf>,
        /// Stream tagged {"kind":"chat"|"status", ...} NDJSON events live to stdout as each match finishes
        #[arg(long)]
        stream: bool,
        /// Total concurrent streaming downloads + parsers
        #[arg(short, long, default_value_t = 64)]
        concurrency: usize,
        /// Max concurrent downloads per Valve replay cluster host (prevents per-host throttling)
        #[arg(long, default_value_t = 6)]
        max_per_host: usize,
        /// Per-match HTTP timeout in seconds
        #[arg(long, default_value_t = 120)]
        timeout: u64,
        /// Max retries on transient HTTP/network failures
        #[arg(long, default_value_t = 2)]
        retries: usize,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Extract {
            input,
            match_id,
            summary,
        } => {
            let result = if input.as_os_str() == "-" {
                decompress::parse_compressed_stream(io::stdin().lock(), match_id)?
            } else {
                let file = File::open(&input)
                    .with_context(|| format!("failed to open {}", input.display()))?;
                decompress::parse_compressed_stream(file, match_id)?
            };

            let stdout = io::stdout();
            let mut out = io::BufWriter::new(stdout.lock());
            if summary {
                serde_json::to_writer_pretty(&mut out, &result)?;
                writeln!(out)?;
            } else {
                for msg in &result.chat_messages {
                    serde_json::to_writer(&mut out, msg)?;
                    writeln!(out)?;
                }
            }
            out.flush()?;
        }
        Commands::Batch {
            input,
            output,
            status,
            stream,
            concurrency,
            max_per_host,
            timeout,
            retries,
        } => {
            batch::run_batch(batch::BatchConfig {
                input_path: &input,
                output_path: output.as_deref(),
                status_path: status.as_deref(),
                stream_stdout: stream,
                concurrency,
                max_per_host,
                timeout_secs: timeout,
                max_retries: retries,
            })
            .await?;
        }
    }

    Ok(())
}
