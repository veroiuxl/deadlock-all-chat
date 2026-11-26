"""Chat extraction backends: ultra-fast Rust streaming engine + boon-deadlock fallback."""

from __future__ import annotations

import bz2
import gzip
import json
import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Iterable

import polars as pl
import zstandard as zstd

from .api import MatchReplayJob

CHAT_SCHEMA: dict[str, pl.DataType] = {
    "match_id": pl.UInt64,
    "start_time": pl.Int64,
    "duration_s": pl.UInt32,
    "game_mode": pl.UInt32,
    "match_mode": pl.UInt32,
    "tick": pl.Int32,
    "game_time_s": pl.Float32,
    "player_slot": pl.Int32,
    "account_id": pl.UInt32,
    "steam_id": pl.UInt64,
    "player_name": pl.Utf8,
    "hero_id": pl.UInt32,
    "team": pl.UInt8,
    "chat_type": pl.Utf8,
    "text": pl.Utf8,
}

STATUS_SCHEMA: dict[str, pl.DataType] = {
    "match_id": pl.UInt64,
    "status": pl.Utf8,
    "http_status": pl.UInt16,
    "bytes_downloaded": pl.UInt64,
    "chat_count": pl.UInt32,
    "player_count": pl.UInt32,
    "duration_ms": pl.UInt64,
    "error": pl.Utf8,
}

STEAM_ID64_OFFSET = 76561197960265728


def empty_chats_df() -> pl.DataFrame:
    return pl.DataFrame(schema=CHAT_SCHEMA)


def empty_status_df() -> pl.DataFrame:
    return pl.DataFrame(schema=STATUS_SCHEMA)


def find_rust_binary() -> Path | None:
    """Locate the compiled `deadlock-chat-extractor` binary."""
    env_bin = os.environ.get("DEADLOCK_CHAT_BIN")
    if env_bin and Path(env_bin).is_file():
        return Path(env_bin)

    # Check relative to repository root
    repo_root = Path(__file__).resolve().parents[2]
    candidates = [
        repo_root / "rust" / "chat-extractor" / "target" / "release" / "deadlock-chat-extractor",
        Path.cwd() / "rust" / "chat-extractor" / "target" / "release" / "deadlock-chat-extractor",
    ]
    for c in candidates:
        if c.is_file():
            return c

    which = shutil.which("deadlock-chat-extractor")
    return Path(which) if which else None


def build_rust_binary(repo_root: Path | None = None) -> Path:
    """Compile `deadlock-chat-extractor` in release mode (~30s on Colab)."""
    root = repo_root or Path(__file__).resolve().parents[2]
    manifest = root / "rust" / "chat-extractor" / "Cargo.toml"
    if not manifest.is_file():
        raise FileNotFoundError(f"Rust extractor Cargo.toml not found at {manifest}")
    subprocess.run(
        ["cargo", "build", "--release", "--manifest-path", str(manifest)],
        check=True,
    )
    binary = root / "rust" / "chat-extractor" / "target" / "release" / "deadlock-chat-extractor"
    if not binary.is_file():
        raise RuntimeError(f"Build succeeded but binary not found at {binary}")
    return binary


def extract_file_rust(path: Path | str, match_id: int = 0) -> pl.DataFrame:
    """Extract chat messages from a local .dem / .dem.bz2 / .dem.zst using the Rust engine."""
    binary = find_rust_binary()
    if binary is None:
        raise FileNotFoundError(
            "deadlock-chat-extractor binary not found. Run `build_rust_binary()` or "
            "`cargo build --release --manifest-path rust/chat-extractor/Cargo.toml`."
        )
    proc = subprocess.run(
        [str(binary), "extract", str(path), "--match-id", str(match_id)],
        capture_output=True,
        text=True,
        check=True,
    )
    rows = [json.loads(line) for line in proc.stdout.splitlines() if line.strip()]
    if not rows:
        return empty_chats_df()
    return pl.DataFrame(rows, schema=CHAT_SCHEMA)


def extract_batch_rust(
    jobs: Iterable[MatchReplayJob],
    concurrency: int = 16,
    timeout_s: int = 120,
    retries: int = 2,
    verbose: bool = True,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Stream-download, decompress, and extract chat messages from a batch of replay URLs.

    Uses zero disk space for uncompressed demos: HTTP chunks stream straight through
    Zstd/Bzip2 decompression into the zero-entity-decoding PBDEMS2 parser in Rust.
    """
    job_list = list(jobs)
    if not job_list:
        return empty_chats_df(), empty_status_df()

    binary = find_rust_binary()
    if binary is None:
        raise FileNotFoundError(
            "deadlock-chat-extractor binary not found. Build it with `build_rust_binary()`."
        )

    with tempfile.TemporaryDirectory(prefix="deadlock_chat_batch_") as tmpdir:
        tmp = Path(tmpdir)
        jobs_file = tmp / "jobs.jsonl"
        out_file = tmp / "chats.ndjson"
        status_file = tmp / "status.ndjson"

        with jobs_file.open("w", encoding="utf-8") as f:
            for j in job_list:
                f.write(json.dumps(j.to_job_dict()) + "\n")

        cmd = [
            str(binary),
            "batch",
            "--input",
            str(jobs_file),
            "--output",
            str(out_file),
            "--status",
            str(status_file),
            "--concurrency",
            str(concurrency),
            "--timeout",
            str(timeout_s),
            "--retries",
            str(retries),
        ]
        subprocess.run(
            cmd,
            check=True,
            stderr=None if verbose else subprocess.DEVNULL,
        )

        chat_rows = []
        if out_file.is_file():
            with out_file.open("r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        chat_rows.append(json.loads(line))

        status_rows = []
        if status_file.is_file():
            with status_file.open("r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        rec = json.loads(line)
                        rec.setdefault("error", None)
                        status_rows.append(rec)

        chats_df = pl.DataFrame(chat_rows, schema=CHAT_SCHEMA) if chat_rows else empty_chats_df()
        status_df = (
            pl.DataFrame(status_rows, schema=STATUS_SCHEMA) if status_rows else empty_status_df()
        )
        return chats_df, status_df


def decompress_to_file(src: Path, dst: Path) -> None:
    """Decompress Zstd, Bzip2, Gzip, or raw demo file by inspecting magic bytes."""
    with src.open("rb") as f:
        magic = f.read(8)

    if magic[:4] == b"\x28\xb5\x2f\xfd":
        dctx = zstd.ZstdDecompressor()
        with src.open("rb") as fin, dst.open("wb") as fout:
            dctx.copy_stream(fin, fout)
    elif magic[:3] == b"BZh":
        with bz2.open(src, "rb") as fin, dst.open("wb") as fout:
            shutil.copyfileobj(fin, fout, length=1024 * 1024)
    elif magic[:2] == b"\x1f\x8b":
        with gzip.open(src, "rb") as fin, dst.open("wb") as fout:
            shutil.copyfileobj(fin, fout, length=1024 * 1024)
    else:
        shutil.copyfile(src, dst)


def extract_file_boon(path: Path | str, match_id: int = 0) -> pl.DataFrame:
    """Fallback / verification extractor using `boon-deadlock` Python bindings.

    Requires `pip install boon-deadlock`. Decompresses compressed replays to a temporary
    .dem file before parsing because Boon memory-maps uncompressed .dem files.
    """
    import boon  # type: ignore[import-not-found]

    src = Path(path)
    with src.open("rb") as f:
        magic = f.read(8)

    with tempfile.TemporaryDirectory(prefix="boon_dem_") as tmpdir:
        if magic == b"PBDEMS2\x00":
            dem_path = src
        else:
            dem_path = Path(tmpdir) / "match.dem"
            decompress_to_file(src, dem_path)

        demo = boon.Demo(str(dem_path))
        chat_df: pl.DataFrame = demo.chat
        if chat_df.is_empty():
            return empty_chats_df()

        players_df: pl.DataFrame = demo.players
        mid = int(getattr(demo, "match_id", 0) or match_id)
        joined = chat_df.join(
            players_df.select(["hero_id", "player_name", "steam_id", "team_num"]).unique(
                subset=["hero_id"], keep="first"
            ),
            on="hero_id",
            how="left",
        )

        rows = []
        for r in joined.iter_rows(named=True):
            sid = int(r["steam_id"] or 0)
            aid = sid - STEAM_ID64_OFFSET if sid >= STEAM_ID64_OFFSET else 0
            tick = int(r["tick"])
            rows.append(
                {
                    "match_id": mid,
                    "start_time": 0,
                    "duration_s": 0,
                    "game_mode": 0,
                    "match_mode": 0,
                    "tick": tick,
                    "game_time_s": float(max(tick, 0)) / 64.0,
                    "player_slot": -1,
                    "account_id": int(aid),
                    "steam_id": sid,
                    "player_name": str(r["player_name"] or ""),
                    "hero_id": int(r["hero_id"] or 0),
                    "team": int(r["team_num"] or 0),
                    "chat_type": str(r["chat_type"]),
                    "text": str(r["text"]),
                }
            )
        return pl.DataFrame(rows, schema=CHAT_SCHEMA)
