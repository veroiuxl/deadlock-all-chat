"""High-level pipelines: resumable bulk harvesting & hybrid on-demand username lookup."""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import polars as pl

from .api import DeadlockApiClient, MatchReplayJob
from .extractor import extract_batch_rust
from .storage import ChatLake


def harvest_bulk(
    lake: ChatLake | Path | str,
    target_matches: int = 500,
    chunk_size: int = 100,
    concurrency: int = 16,
    days_back: int = 14,
    source: str = "lake",
    api_key: str | None = None,
    compact_every_chunks: int = 10,
    verbose: bool = True,
) -> dict[str, Any]:
    """Resumably harvest chat messages from Deadlock replays into a Parquet ChatLake.

    Parameters
    ----------
    lake:
        `ChatLake` instance or path to storage directory (e.g. Google Drive folder).
    target_matches:
        Number of new matches to attempt in this run.
    chunk_size:
        Number of replays per checkpointed batch (flushes Parquet shards after every chunk).
    concurrency:
        Number of simultaneous async HTTP + decompression + parse streams in Rust.
    days_back:
        How many days back to query `match_salts` (Valve keeps replays ~14-21 days).
    source:
        `"lake"` (Deadlock API public `match_salts` Parquet, best for large bulk runs)
        or `"live"` (`/v1/matches/recently-fetched`, best for ultra-recent matches).
    """
    chat_lake = lake if isinstance(lake, ChatLake) else ChatLake(lake)
    started = time.time()

    seen = chat_lake.get_processed_match_ids()
    if verbose:
        print(f"[harvest] Already processed {len(seen):,} matches in {chat_lake.root}")

    with DeadlockApiClient(api_key=api_key) as api:
        if source == "live":
            if verbose:
                print(f"[harvest] Fetching up to {target_matches} live matches from /v1/matches/recently-fetched...")
            jobs = api.fetch_live_recent_matches(
                limit=target_matches,
                max_age_hours=days_back * 24.0,
                exclude_match_ids=seen,
            )
        else:
            if verbose:
                print(
                    f"[harvest] Querying Deadlock API match_salts Parquet lake "
                    f"(days_back={days_back}, target={target_matches})..."
                )
            jobs = api.fetch_recent_replay_jobs_from_lake(
                days_back=days_back,
                limit=target_matches,
                exclude_match_ids=seen,
            )

    if verbose:
        print(f"[harvest] Discovered {len(jobs):,} new candidate matches with replay_salt")

    total_chats = 0
    total_ok = 0
    total_404 = 0
    total_err = 0
    total_bytes = 0
    chunks_done = 0

    for i in range(0, len(jobs), chunk_size):
        chunk: list[MatchReplayJob] = jobs[i : i + chunk_size]
        chunk_idx = i // chunk_size + 1
        total_chunks = (len(jobs) + chunk_size - 1) // chunk_size
        if verbose:
            print(
                f"\n[harvest] Chunk {chunk_idx}/{total_chunks} "
                f"({len(chunk)} matches, concurrency={concurrency})..."
            )

        chats_df, status_df = extract_batch_rust(
            chunk,
            concurrency=concurrency,
            verbose=verbose,
        )

        chat_lake.append_chats(chats_df)
        chat_lake.append_status(status_df)

        chunks_done += 1
        total_chats += len(chats_df)
        if not status_df.is_empty():
            counts = dict(status_df.group_by("status").len().iter_rows())
            total_ok += counts.get("ok", 0) + counts.get("no_chat", 0)
            total_404 += counts.get("http_404", 0)
            total_err += counts.get("error", 0)
            total_bytes += int(status_df["bytes_downloaded"].sum() or 0)

        if compact_every_chunks > 0 and chunks_done % compact_every_chunks == 0:
            if verbose:
                print("[harvest] Compacting Parquet shards...")
            chat_lake.compact()

    elapsed = max(time.time() - started, 0.001)
    summary = {
        "attempted_matches": len(jobs),
        "parsed_ok": total_ok,
        "http_404": total_404,
        "errors": total_err,
        "chat_messages_extracted": total_chats,
        "mb_downloaded": round(total_bytes / 1e6, 2),
        "elapsed_s": round(elapsed, 2),
        "matches_per_sec": round(len(jobs) / elapsed, 2),
        "mb_per_sec": round((total_bytes / 1e6) / elapsed, 2),
    }
    if verbose:
        print(f"\n[harvest] Finished: {summary}")
    return summary


def lookup_player_chats(
    lake: ChatLake | Path | str,
    username: str,
    exact: bool = False,
    live_fetch: bool = True,
    include_historical_names: bool = False,
    days_back: int = 21,
    max_live_matches: int = 30,
    concurrency: int = 12,
    chat_type: str | None = None,
    limit: int = 500,
    api_key: str | None = None,
    verbose: bool = True,
) -> dict[str, Any]:
    """Query all chat messages for a username, optionally live-fetching uncached recent matches.

    Steps:
      1. Resolve `username` -> matching Steam `account_id`(s) via Deadlock API.
      2. If `live_fetch=True`, pull recent match history for resolved account(s), skip
         matches already stored in `ChatLake`, stream-parse missing matches in parallel,
         and persist ALL chat messages from those matches into `ChatLake`.
      3. Query `ChatLake` via DuckDB for all messages matching either `player_name` or
         any resolved `account_id` (catches messages sent under previous Steam names!).
    """
    chat_lake = lake if isinstance(lake, ChatLake) else ChatLake(lake)
    resolved_accounts: list[dict[str, Any]] = []
    live_fetched_matches = 0
    live_extracted_chats = 0

    with DeadlockApiClient(api_key=api_key) as api:
        api_accounts = api.resolve_username(
            username,
            exact=exact,
            limit=10,
            include_historical_lake=include_historical_names,
        )
        local_accounts = chat_lake.find_account_ids_by_username(
            username,
            exact=exact,
            limit=10,
        )
        merged: dict[int, dict[str, Any]] = {}
        for acc in api_accounts + local_accounts:
            aid = int(acc["account_id"])
            if aid not in merged:
                merged[aid] = acc
        resolved_accounts = list(merged.values())
        account_ids = list(merged.keys())

        if verbose:
            print(f"[lookup] Resolved '{username}' -> {len(resolved_accounts)} Steam account(s)")
            for acc in resolved_accounts[:5]:
                print(f"         account_id={acc['account_id']} name={acc['personaname']!r} ({acc['source']})")

        if live_fetch and account_ids:
            seen = chat_lake.get_processed_match_ids()
            all_jobs: dict[int, MatchReplayJob] = {}
            # Live-fetch for up to top 3 resolved accounts to avoid over-fetching common substrings
            for aid in account_ids[:3]:
                jobs = api.get_player_recent_replay_jobs(
                    account_id=aid,
                    days_back=days_back,
                    max_matches=max_live_matches,
                    exclude_match_ids=seen,
                )
                for j in jobs:
                    all_jobs[j.match_id] = j

            job_list = list(all_jobs.values())[:max_live_matches]
            if job_list:
                if verbose:
                    print(
                        f"[lookup] Live-fetching {len(job_list)} uncached recent replay(s) "
                        f"with concurrency={concurrency}..."
                    )
                chats_df, status_df = extract_batch_rust(
                    job_list,
                    concurrency=concurrency,
                    verbose=verbose,
                )
                chat_lake.append_chats(chats_df)
                chat_lake.append_status(status_df)
                live_fetched_matches = len(job_list)
                live_extracted_chats = len(chats_df)
            elif verbose:
                print("[lookup] All recent matches with replay salts are already cached locally.")

    messages_df: pl.DataFrame = chat_lake.query_by_username(
        username=username,
        exact=exact,
        account_ids=account_ids or None,
        chat_type=chat_type,
        limit=limit,
    )

    return {
        "username": username,
        "resolved_accounts": resolved_accounts,
        "live_fetched_matches": live_fetched_matches,
        "live_extracted_chats_total": live_extracted_chats,
        "messages": messages_df,
    }
