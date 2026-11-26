# Deadlock All-Chat

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/veroiuxl/deadlock-all-chat/blob/main/notebooks/deadlock_all_chat_colab.ipynb)

High-throughput collector and username query engine for **Deadlock** in-game replay chat messages (both **All-Chat** and **Team-Chat**), engineered specifically for **Google Colab** and backed by the [Deadlock API](https://api.deadlock-api.com/) and its public [Parquet Data Lake](https://deadlock-api.com/data-dumps).

## Architecture

```mermaid
flowchart LR
    API["Deadlock API & Public Data Lake<br/>(match_salts, steam-search,<br/>steam_profile_observed_names)"]
    CDN["Valve Replay CDN<br/>(replay*.valve.net<br/>.dem.bz2 / Zstd streams)"]
    RUST["Rust Streaming Extractor<br/>(deadlock-chat-extractor)<br/>16-32 concurrent streams<br/>0 temp disk writes<br/>skips svc_PacketEntities"]
    LAKE["Google Drive Parquet Lake<br/>chats/*.parquet (Zstd, ~1 KB/match)<br/>state/*.parquet (checkpoints)"]
    QUERY["DuckDB Query Engine<br/>Username / Account ID /<br/>Full-Text / Custom SQL"]

    API -->|Match IDs + Replay Salts| RUST
    CDN -->|HTTP Stream| RUST
    RUST -->|NDJSON -> Polars| LAKE
    LAKE --> QUERY
    API -.->|On-Demand Username -> Account ID| QUERY
```

## Why This Processing Strategy Is Best for Google Colab

We evaluated three approaches on real Deadlock replays and built the fastest one into a Colab-friendly pipeline:

| Approach | Raw `.dem` Parse (52 MB) | Compressed `.dem.bz2` End-to-End | Temp Disk Per Replay | Colab Suitability |
| :--- | ---: | ---: | ---: | :--- |
| **Custom Rust Streaming Parser (`deadlock-chat-extractor`)** | **42 ms** | **182 ms** | **0 MB** | **Best** — 30× faster CPU, zero disk writes, 16–32 concurrent HTTP streams |
| [`boon-deadlock`](https://pypi.org/project/boon-deadlock/) (Python/Rust full parser) | 1,304 ms | 1,392 ms | 50–450 MB | Good zero-compile fallback (included in `extractor.py`) |
| Deadlock API `POST /v1/matches/demo/query` | — | ~13,000 ms (server-side) | 0 MB | Ad-hoc single match only (rate-limited queue) |

### Key Technical Findings That Drive the Design

1. **Zero-Entity-Decoding Source 2 Parsing (30× CPU speedup):**
   ~98% of bytes in a Deadlock `.dem` file are `svc_PacketEntities` entity delta updates. Chat messages live in `CCitadelUserMsg_ChatMsg` (message ID `314`), player identities (`name`, `steamid`, `account_id`) live in the `userinfo` string table (`DEM_StringTables` / `DEM_FullPacket`), and match metadata (`match_id`, `start_time`, `duration_s`, `hero_id`, `team`) lives in `CCitadelUserMsg_PostMatchDetails` (ID `316`). Skipping all other messages in `O(1)` inside `CDemoPacket` cuts CPU time from ~1.3s to **~42 ms** while maintaining **100% parity** with `boon-deadlock`.
2. **Magic-Byte Decompression (Zstd disguised as `.dem.bz2`):**
   Even though Valve replay URLs still end in `.dem.bz2`, newer Deadlock replays are actually **Zstandard** compressed (`28 B5 2F FD`, decompressing at >300 MB/s), while older replays use **Bzip2** (`BZh`). Our streaming decompressor sniffs magic bytes automatically and pipes HTTP chunks directly into the parser with **zero uncompressed `.dem` disk writes**.
3. **High-Throughput Colab Engine (v0.2):**
   - **Use a standard CPU runtime** (no GPU needed — workload is network I/O + Zstd/Snappy CPU).
   - **Continuous streaming pool + per-cluster round-robin scheduler:** Keeps `64`+ concurrent HTTP streams 100% saturated (no chunk straggler barriers!) while capping connections per Valve replay cluster (`--max-per-host 6` across Valve's ~68 clusters) to avoid per-host throttling.
   - **Local `/tmp` staging & `match_salts` caching:** Caches recent `match_salts` locally in `/tmp` so repeat runs start in <1s, and stages Zstd Parquet writes to `/tmp` before moving to Google Drive FUSE.
   - **Multi-worker sharding (`--worker-id / --num-workers`):** Run *N* Colab tabs or cloud VMs in parallel against the same storage directory with zero duplicate downloads (`match_id % num_workers == worker_id`).
   - Extracted chat data compresses to **~0.6–2 KB per match** in Zstd Parquet (**~70,000× smaller** than the source replays — 1,000,000 matches fit in ~1–2 GB on Google Drive).
4. **Hybrid Collection + On-Demand Username Query:**
   - **Bulk Mode:** Continuously harvests matches from Deadlock API's public `match_salts` Parquet lake via DuckDB HTTP range queries.
   - **Username Lookup Mode:** Queries your local Parquet lake by username **and** resolves the player's Steam `account_id` via Deadlock API (`/v1/players/steam-search` + optional historical `steam_profile_observed_names`), optionally live-fetching & parsing any of their uncached recent replays in seconds and saving all players' messages back into the lake.

## Quickstart in Google Colab

1. Open [`notebooks/deadlock_all_chat_colab.ipynb`](notebooks/deadlock_all_chat_colab.ipynb) in Google Colab (Runtime → **CPU**).
2. Run **Cell 1** to mount Google Drive, install dependencies, and compile/cache `deadlock-chat-extractor-v0.2`.
3. Run **Cell 2** to bulk-harvest recent matches continuously into your Drive Parquet lake (resumable across sessions; set `NUM_WORKERS > 1` across multiple Colab tabs for linear scaling).
4. Run **Cell 3** to query any Steam username (with optional on-demand live fetching of their recent matches).

## Local CLI Usage

```bash
# 1. Install Python package
pip install -e .

# 2. Build the Rust streaming extractor (requires Rust/Cargo)
deadlock-chat build

# 3. Bulk-harvest 2000 recent matches continuously into ./data
deadlock-chat --data-dir ./data harvest --matches 2000 --concurrency 64 --max-per-host 6 --flush-every 250

# 3b. Scale across 3 parallel workers sharing ./data (run in 3 terminals/machines)
deadlock-chat --data-dir ./data harvest --matches 5000 --worker-id 0 --num-workers 3
deadlock-chat --data-dir ./data harvest --matches 5000 --worker-id 1 --num-workers 3
deadlock-chat --data-dir ./data harvest --matches 5000 --worker-id 2 --num-workers 3

# 4. Query all chat messages typed by a username (with live fetch of uncached recent games)
deadlock-chat --data-dir ./data lookup "SomeUsername"

# 5. Query local Parquet lake only (no network requests)
deadlock-chat --data-dir ./data lookup "SomeUsername" --no-live-fetch

# 6. Full-text search across all collected messages
deadlock-chat --data-dir ./data search "mid boss"

# 7. View corpus statistics & compact Parquet shards
deadlock-chat --data-dir ./data stats
deadlock-chat --data-dir ./data compact
```

## Parquet Schema (`chats/*.parquet`)

| Column | Type | Description |
| :--- | :--- | :--- |
| `match_id` | `UInt64` | Deadlock match ID |
| `start_time` | `Int64` | Match start UNIX timestamp (UTC seconds) |
| `duration_s` | `UInt32` | Match duration in seconds |
| `game_mode` / `match_mode` | `UInt32` | Deadlock game/match mode enum |
| `tick` | `Int32` | Source 2 demo tick when message was sent |
| `game_time_s` | `Float32` | In-game timestamp in seconds (`tick * tick_interval`) |
| `player_slot` | `Int32` | 0-based `userinfo` slot index |
| `account_id` | `UInt32` | SteamID3 (`steam_id - 76561197960265728`) |
| `steam_id` | `UInt64` | SteamID64 |
| `player_name` | `Utf8` | In-game username used during that match |
| `hero_id` | `UInt32` | Hero ID from `PostMatchDetails` |
| `team` | `UInt8` | Lobby team (`0` = Team 0 / Amber, `1` = Team 1 / Sapphire) |
| `chat_type` | `Utf8` | `"all"` or `"team"` |
| `text` | `Utf8` | Chat message content |

## Running Tests

```bash
pip install -e ".[dev]"
pytest -v
```
