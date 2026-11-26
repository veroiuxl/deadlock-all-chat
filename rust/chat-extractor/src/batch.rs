use anyhow::{Context, Result};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::task::JoinSet;

use crate::decompress::{parse_compressed_stream, ChannelReader};
use crate::parser::ChatMessageRecord;

#[derive(Debug, Clone, Deserialize)]
pub struct MatchJob {
    pub match_id: u64,
    pub demo_url: String,
    #[serde(default)]
    pub start_time: Option<i64>,
    #[serde(default)]
    pub cluster_id: Option<u32>,
}

#[derive(Debug, Clone, Serialize)]
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

#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum StreamEvent<'a> {
    Chat(&'a ChatMessageRecord),
    Status(&'a MatchStatusRecord),
}

struct CompletedJob {
    host_key: String,
    status: MatchStatusRecord,
    chats: Vec<ChatMessageRecord>,
}

/// Round-robin per-cluster scheduler that keeps all global worker slots saturated
/// while preventing head-of-line blocking and per-cluster Valve CDN hot-spotting.
struct ClusterScheduler {
    queues: HashMap<String, VecDeque<MatchJob>>,
    hosts: Vec<String>,
    cursor: usize,
    active_per_host: HashMap<String, usize>,
    max_per_host: usize,
    remaining: usize,
}

impl ClusterScheduler {
    fn new(jobs: Vec<MatchJob>, max_per_host: usize) -> Self {
        let mut queues: HashMap<String, VecDeque<MatchJob>> = HashMap::new();
        let remaining = jobs.len();
        for job in jobs {
            let host = extract_host_key(&job);
            queues.entry(host).or_default().push_back(job);
        }
        let mut hosts: Vec<String> = queues.keys().cloned().collect();
        hosts.sort();
        Self {
            queues,
            hosts,
            cursor: 0,
            active_per_host: HashMap::new(),
            max_per_host: max_per_host.max(1),
            remaining,
        }
    }

    fn has_pending(&self) -> bool {
        self.remaining > 0
    }

    /// Pop the next eligible job whose host currently has fewer than `max_per_host`
    /// active downloads. Falls back to any host if `allow_overflow` is true (e.g., when
    /// only 1-2 clusters remain at the very tail of a run).
    fn pop_next(&mut self, allow_overflow: bool) -> Option<(String, MatchJob)> {
        if self.hosts.is_empty() || self.remaining == 0 {
            return None;
        }
        let n = self.hosts.len();
        for offset in 0..n {
            let idx = (self.cursor + offset) % n;
            let host = &self.hosts[idx];
            let active = *self.active_per_host.get(host).unwrap_or(&0);
            if !allow_overflow && active >= self.max_per_host {
                continue;
            }
            if let Some(q) = self.queues.get_mut(host) {
                if let Some(job) = q.pop_front() {
                    let host_owned = host.clone();
                    *self.active_per_host.entry(host_owned.clone()).or_insert(0) += 1;
                    self.remaining -= 1;
                    self.cursor = (idx + 1) % n;
                    return Some((host_owned, job));
                }
            }
        }
        None
    }

    fn mark_done(&mut self, host: &str) {
        if let Some(c) = self.active_per_host.get_mut(host) {
            *c = c.saturating_sub(1);
        }
    }
}

fn extract_host_key(job: &MatchJob) -> String {
    if let Some(cid) = job.cluster_id {
        return format!("cluster_{}", cid);
    }
    // Parse hostname from http://replay142.valve.net/...
    if let Some(rest) = job
        .demo_url
        .strip_prefix("http://")
        .or_else(|| job.demo_url.strip_prefix("https://"))
    {
        if let Some((host, _)) = rest.split_once('/') {
            return host.to_string();
        }
    }
    "default".to_string()
}

pub struct BatchConfig<'a> {
    pub input_path: &'a Path,
    pub output_path: Option<&'a Path>,
    pub status_path: Option<&'a Path>,
    pub stream_stdout: bool,
    pub concurrency: usize,
    pub max_per_host: usize,
    pub timeout_secs: u64,
    pub max_retries: usize,
}

pub async fn run_batch(cfg: BatchConfig<'_>) -> Result<()> {
    let jobs = load_jobs(cfg.input_path)?;
    let total_jobs = jobs.len() as u64;
    if total_jobs == 0 {
        return Ok(());
    }

    let concurrency = cfg.concurrency.max(1);
    let max_per_host = cfg.max_per_host.max(1).min(concurrency);

    let client = Arc::new(
        reqwest::Client::builder()
            .user_agent("deadlock-all-chat/0.2")
            .connect_timeout(Duration::from_secs(15))
            .timeout(Duration::from_secs(cfg.timeout_secs))
            .pool_max_idle_per_host(max_per_host)
            .tcp_keepalive(Duration::from_secs(30))
            .build()
            .context("failed to build HTTP client")?,
    );

    let mut out_writer = match cfg.output_path {
        Some(p) => {
            let f = File::create(p)
                .with_context(|| format!("failed to create output file {}", p.display()))?;
            Some(BufWriter::with_capacity(256 * 1024, f))
        }
        None => None,
    };

    let mut status_writer = match cfg.status_path {
        Some(p) => {
            let f = File::create(p)
                .with_context(|| format!("failed to create status file {}", p.display()))?;
            Some(BufWriter::with_capacity(64 * 1024, f))
        }
        None => None,
    };

    let stdout_handle = io::stdout();
    let mut stream_writer = if cfg.stream_stdout {
        Some(BufWriter::with_capacity(64 * 1024, stdout_handle.lock()))
    } else {
        None
    };

    let mut scheduler = ClusterScheduler::new(jobs, max_per_host);
    let unique_clusters = scheduler.hosts.len();
    eprintln!(
        "[batch] Starting {} match(es) across {} cluster(s) | concurrency={} max_per_host={}",
        total_jobs, unique_clusters, concurrency, max_per_host
    );

    let mut join_set: JoinSet<CompletedJob> = JoinSet::new();
    let mut completed: u64 = 0;
    let mut total_chats: u64 = 0;
    let mut total_bytes: u64 = 0;
    let mut ok_count: u64 = 0;
    let mut not_found_count: u64 = 0;
    let mut err_count: u64 = 0;
    let start_all = Instant::now();
    let log_every = (total_jobs / 10).clamp(1, 50);

    // Helper closure-style loop to keep `join_set` saturated up to `concurrency`
    let spawn_ready = |scheduler: &mut ClusterScheduler,
                       join_set: &mut JoinSet<CompletedJob>,
                       client: &Arc<reqwest::Client>| {
        while join_set.len() < concurrency && scheduler.has_pending() {
            // First try strict per-host cap; only allow overflow if nothing is in flight
            // so we never deadlock when all remaining jobs belong to a single cluster.
            let next = scheduler
                .pop_next(false)
                .or_else(|| {
                    if join_set.is_empty() {
                        scheduler.pop_next(true)
                    } else {
                        None
                    }
                });
            let Some((host_key, job)) = next else {
                break;
            };
            let client = client.clone();
            let retries = cfg.max_retries;
            join_set.spawn(async move {
                let (status, chats) = process_single_job(&client, &job, retries).await;
                CompletedJob {
                    host_key,
                    status,
                    chats,
                }
            });
        }
    };

    spawn_ready(&mut scheduler, &mut join_set, &client);

    while let Some(res) = join_set.join_next().await {
        let done_job = match res {
            Ok(j) => j,
            Err(_) => continue,
        };

        scheduler.mark_done(&done_job.host_key);

        // Immediately refill the freed slot BEFORE doing any I/O so downloads never stall!
        spawn_ready(&mut scheduler, &mut join_set, &client);

        total_bytes += done_job.status.bytes_downloaded;
        match done_job.status.status.as_str() {
            "ok" | "no_chat" => {
                ok_count += 1;
                total_chats += done_job.status.chat_count as u64;
            }
            "http_404" => not_found_count += 1,
            _ => err_count += 1,
        }

        if let Some(w) = out_writer.as_mut() {
            for rec in &done_job.chats {
                serde_json::to_writer(&mut *w, rec)?;
                w.write_all(b"\n")?;
            }
        }

        if let Some(sw) = status_writer.as_mut() {
            serde_json::to_writer(&mut *sw, &done_job.status)?;
            sw.write_all(b"\n")?;
        }

        if let Some(st) = stream_writer.as_mut() {
            for rec in &done_job.chats {
                serde_json::to_writer(&mut *st, &StreamEvent::Chat(rec))?;
                st.write_all(b"\n")?;
            }
            serde_json::to_writer(&mut *st, &StreamEvent::Status(&done_job.status))?;
            st.write_all(b"\n")?;
            st.flush()?;
        }

        completed += 1;
        if completed % log_every == 0 || completed == total_jobs {
            let elapsed = start_all.elapsed().as_secs_f64().max(0.001);
            let mb = total_bytes as f64 / 1e6;
            eprintln!(
                "[batch] {}/{} done (in_flight={}) | ok={} 404={} err={} | chats={} | {:.1} MB ({:.1} MB/s) | {:.2} matches/s",
                completed,
                total_jobs,
                join_set.len(),
                ok_count,
                not_found_count,
                err_count,
                total_chats,
                mb,
                mb / elapsed,
                completed as f64 / elapsed,
            );
        }
    }

    if let Some(w) = out_writer.as_mut() {
        w.flush()?;
    }
    if let Some(sw) = status_writer.as_mut() {
        sw.flush()?;
    }
    if let Some(st) = stream_writer.as_mut() {
        st.flush()?;
    }

    Ok(())
}

async fn process_single_job(
    client: &reqwest::Client,
    job: &MatchJob,
    max_retries: usize,
) -> (MatchStatusRecord, Vec<ChatMessageRecord>) {
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
                    result.chat_messages,
                );
            }
            Ok(Err(e)) => {
                last_err = Some(stream_err.unwrap_or_else(|| format!("parse error: {:#}", e)));
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
