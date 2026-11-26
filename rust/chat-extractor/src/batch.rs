use anyhow::{Context, Result};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Mutex, Semaphore};

use crate::decompress::{parse_compressed_stream, ChannelReader};

#[derive(Debug, Deserialize)]
pub struct MatchJob {
    pub match_id: u64,
    pub demo_url: String,
    #[serde(default)]
    pub start_time: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct MatchStatusRecord {
    pub match_id: u64,
    pub status: String,
    pub http_status: u16,
    pub bytes_downloaded: u64,
    pub chat_count: usize,
    pub player_count: usize,
    pub duration_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

pub async fn run_batch(
    input_path: &Path,
    output_path: &Path,
    status_path: Option<&Path>,
    concurrency: usize,
    timeout_secs: u64,
    max_retries: usize,
) -> Result<()> {
    let jobs = load_jobs(input_path)?;
    let total_jobs = jobs.len() as u64;
    if total_jobs == 0 {
        return Ok(());
    }

    let client = reqwest::Client::builder()
        .user_agent("deadlock-all-chat/0.1")
        .connect_timeout(Duration::from_secs(15))
        .timeout(Duration::from_secs(timeout_secs))
        .pool_max_idle_per_host(concurrency.min(8))
        .build()
        .context("failed to build HTTP client")?;

    let out_file = File::create(output_path)
        .with_context(|| format!("failed to create output file {}", output_path.display()))?;
    let out_writer = Arc::new(Mutex::new(BufWriter::with_capacity(256 * 1024, out_file)));

    let status_writer = match status_path {
        Some(p) => {
            let f = File::create(p)
                .with_context(|| format!("failed to create status file {}", p.display()))?;
            Some(Arc::new(Mutex::new(BufWriter::with_capacity(64 * 1024, f))))
        }
        None => None,
    };

    let sem = Arc::new(Semaphore::new(concurrency.max(1)));
    let completed = Arc::new(AtomicU64::new(0));
    let total_chats = Arc::new(AtomicU64::new(0));
    let total_bytes = Arc::new(AtomicU64::new(0));
    let ok_count = Arc::new(AtomicU64::new(0));
    let not_found_count = Arc::new(AtomicU64::new(0));
    let err_count = Arc::new(AtomicU64::new(0));
    let start_all = Instant::now();

    let mut handles = Vec::with_capacity(jobs.len());

    for job in jobs {
        let permit = sem.clone().acquire_owned().await?;
        let client = client.clone();
        let out_writer = out_writer.clone();
        let status_writer = status_writer.clone();
        let completed = completed.clone();
        let total_chats = total_chats.clone();
        let total_bytes = total_bytes.clone();
        let ok_count = ok_count.clone();
        let not_found_count = not_found_count.clone();
        let err_count = err_count.clone();

        let handle = tokio::spawn(async move {
            let _permit = permit;
            let job_start = Instant::now();
            let (status_rec, chat_lines) = process_single_job(&client, &job, max_retries).await;

            total_bytes.fetch_add(status_rec.bytes_downloaded, Ordering::Relaxed);
            match status_rec.status.as_str() {
                "ok" | "no_chat" => {
                    ok_count.fetch_add(1, Ordering::Relaxed);
                    total_chats.fetch_add(status_rec.chat_count as u64, Ordering::Relaxed);
                }
                "http_404" => {
                    not_found_count.fetch_add(1, Ordering::Relaxed);
                }
                _ => {
                    err_count.fetch_add(1, Ordering::Relaxed);
                }
            }

            if !chat_lines.is_empty() {
                let mut w = out_writer.lock().await;
                for line in &chat_lines {
                    let _ = w.write_all(line.as_bytes());
                    let _ = w.write_all(b"\n");
                }
            }

            if let Some(sw) = &status_writer {
                if let Ok(line) = serde_json::to_string(&status_rec) {
                    let mut w = sw.lock().await;
                    let _ = w.write_all(line.as_bytes());
                    let _ = w.write_all(b"\n");
                }
            }

            let done = completed.fetch_add(1, Ordering::Relaxed) + 1;
            let log_every = (total_jobs / 5).clamp(1, 25);
            if done % log_every == 0 || done == total_jobs {
                let elapsed = start_all.elapsed().as_secs_f64().max(0.001);
                let mb = total_bytes.load(Ordering::Relaxed) as f64 / 1e6;
                eprintln!(
                    "[batch] {}/{} done | ok={} 404={} err={} | chats={} | {:.1} MB ({:.1} MB/s) | {:.2} matches/s (last={}ms)",
                    done,
                    total_jobs,
                    ok_count.load(Ordering::Relaxed),
                    not_found_count.load(Ordering::Relaxed),
                    err_count.load(Ordering::Relaxed),
                    total_chats.load(Ordering::Relaxed),
                    mb,
                    mb / elapsed,
                    done as f64 / elapsed,
                    job_start.elapsed().as_millis(),
                );
            }
        });
        handles.push(handle);
    }

    for h in handles {
        let _ = h.await;
    }

    out_writer.lock().await.flush()?;
    if let Some(sw) = &status_writer {
        sw.lock().await.flush()?;
    }

    Ok(())
}

async fn process_single_job(
    client: &reqwest::Client,
    job: &MatchJob,
    max_retries: usize,
) -> (MatchStatusRecord, Vec<String>) {
    let started = Instant::now();
    let mut last_http: u16 = 0;
    let mut last_err: Option<String> = None;

    for attempt in 0..=max_retries {
        if attempt > 0 {
            let backoff_ms = 400 * (1u64 << (attempt - 1).min(4));
            tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
        }

        let resp = match client.get(&job.demo_url).send().await {
            Ok(r) => r,
            Err(e) => {
                last_err = Some(format!("request error: {}", e));
                continue;
            }
        };

        last_http = resp.status().as_u16();
        if last_http == 404 || last_http == 410 {
            return (
                MatchStatusRecord {
                    match_id: job.match_id,
                    status: "http_404".to_string(),
                    http_status: last_http,
                    bytes_downloaded: 0,
                    chat_count: 0,
                    player_count: 0,
                    duration_ms: started.elapsed().as_millis() as u64,
                    error: None,
                },
                Vec::new(),
            );
        }

        if !resp.status().is_success() {
            last_err = Some(format!("HTTP {}", last_http));
            if last_http == 429 || last_http >= 500 {
                continue;
            }
            break;
        }

        // Stream HTTP body directly into blocking zstd/bzip2 + PBDEMS2 parser (0 disk writes!)
        let (tx, rx) = mpsc::channel::<Result<Vec<u8>, String>>(32);
        let fallback_mid = job.match_id;
        let parse_task = tokio::task::spawn_blocking(move || {
            let reader = ChannelReader::new(rx);
            parse_compressed_stream(reader, fallback_mid)
        });

        let mut stream = resp.bytes_stream();
        let mut downloaded: u64 = 0;
        let mut stream_err: Option<String> = None;

        while let Some(item) = stream.next().await {
            match item {
                Ok(chunk) => {
                    downloaded += chunk.len() as u64;
                    if tx.send(Ok(chunk.to_vec())).await.is_err() {
                        // Parser exited early (e.g., DEM_Stop or parse error)
                        break;
                    }
                }
                Err(e) => {
                    let msg = format!("stream error: {}", e);
                    stream_err = Some(msg.clone());
                    let _ = tx.send(Err(msg)).await;
                    break;
                }
            }
        }
        drop(tx);

        match parse_task.await {
            Ok(Ok(mut result)) => {
                if result.start_time == 0 {
                    if let Some(st) = job.start_time {
                        result.start_time = st;
                        for c in &mut result.chat_messages {
                            c.start_time = st;
                        }
                    }
                }
                let chat_count = result.chat_messages.len();
                let mut lines = Vec::with_capacity(chat_count);
                for rec in &result.chat_messages {
                    if let Ok(s) = serde_json::to_string(rec) {
                        lines.push(s);
                    }
                }
                return (
                    MatchStatusRecord {
                        match_id: job.match_id,
                        status: if chat_count > 0 {
                            "ok".to_string()
                        } else {
                            "no_chat".to_string()
                        },
                        http_status: last_http,
                        bytes_downloaded: downloaded,
                        chat_count,
                        player_count: result.player_count,
                        duration_ms: started.elapsed().as_millis() as u64,
                        error: None,
                    },
                    lines,
                );
            }
            Ok(Err(e)) => {
                last_err = Some(stream_err.unwrap_or_else(|| format!("parse error: {:#}", e)));
                // Retry transient network truncation, not invalid magic bytes
                if last_err
                    .as_deref()
                    .unwrap_or("")
                    .contains("invalid demo magic")
                {
                    break;
                }
            }
            Err(e) => {
                last_err = Some(format!("join error: {}", e));
            }
        }
    }

    (
        MatchStatusRecord {
            match_id: job.match_id,
            status: "error".to_string(),
            http_status: last_http,
            bytes_downloaded: 0,
            chat_count: 0,
            player_count: 0,
            duration_ms: started.elapsed().as_millis() as u64,
            error: last_err,
        },
        Vec::new(),
    )
}

fn load_jobs(path: &Path) -> Result<Vec<MatchJob>> {
    let reader: Box<dyn BufRead> = if path.as_os_str() == "-" {
        Box::new(BufReader::new(std::io::stdin()))
    } else {
        let f = File::open(path)
            .with_context(|| format!("failed to open job file {}", path.display()))?;
        Box::new(BufReader::new(f))
    };

    let mut jobs = Vec::new();
    for (idx, line) in reader.lines().enumerate() {
        let line = line?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let job: MatchJob = serde_json::from_str(trimmed)
            .with_context(|| format!("invalid JSON on line {}", idx + 1))?;
        jobs.push(job);
    }
    Ok(jobs)
}
