"""Command-line interface for deadlock-all-chat."""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

from .extractor import build_rust_binary, extract_file_boon, extract_file_rust, find_rust_binary
from .pipeline import harvest_bulk, lookup_player_chats
from .storage import ChatLake


def _ensure_binary(auto_build: bool = True) -> None:
    if find_rust_binary() is None:
        if auto_build:
            print("[cli] Rust binary not found; compiling release binary...")
            build_rust_binary()
        else:
            raise SystemExit(
                "deadlock-chat-extractor binary not found. Run `deadlock-chat build` first."
            )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="deadlock-chat",
        description="Deadlock Replay Chat Collector & Username Query Engine",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("./data"),
        help="Path to Parquet ChatLake directory (e.g. Google Drive folder in Colab)",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    # build
    sub.add_parser("build", help="Compile the Rust streaming extractor in release mode")

    # harvest
    p_harv = sub.add_parser("harvest", help="Bulk-harvest chat messages from recent Deadlock replays")
    p_harv.add_argument("--matches", type=int, default=200, help="Number of new matches to attempt")
    p_harv.add_argument("--chunk-size", type=int, default=50, help="Matches per checkpointed shard")
    p_harv.add_argument("--concurrency", type=int, default=16, help="Concurrent HTTP+parse streams")
    p_harv.add_argument("--days-back", type=int, default=14, help="Lookback window in days")
    p_harv.add_argument("--source", choices=["lake", "live"], default="lake")
    p_harv.add_argument("--api-key", default=None)

    # lookup
    p_look = sub.add_parser("lookup", help="Query all chat messages typed by a username")
    p_look.add_argument("username", help="Steam username (or substring)")
    p_look.add_argument("--exact", action="store_true", help="Require exact case-insensitive match")
    p_look.add_argument("--no-live-fetch", action="store_true", help="Only query local Parquet lake")
    p_look.add_argument(
        "--historical-names",
        action="store_true",
        help="Also scan Deadlock API steam_profile_observed_names for past usernames",
    )
    p_look.add_argument("--days-back", type=int, default=21)
    p_look.add_argument("--max-live-matches", type=int, default=25)
    p_look.add_argument("--concurrency", type=int, default=12)
    p_look.add_argument("--chat-type", choices=["all", "team"], default=None)
    p_look.add_argument("--limit", type=int, default=200)
    p_look.add_argument("--api-key", default=None)

    # search
    p_search = sub.add_parser("search", help="Search collected chat messages by keyword")
    p_search.add_argument("query", help="Substring to search for (case-insensitive)")
    p_search.add_argument("--chat-type", choices=["all", "team"], default=None)
    p_search.add_argument("--limit", type=int, default=100)

    # stats
    sub.add_parser("stats", help="Show local ChatLake corpus statistics")

    # compact
    sub.add_parser("compact", help="Compact Parquet shards into sorted, deduplicated files")

    # benchmark
    p_bench = sub.add_parser("benchmark", help="Benchmark Rust streaming extractor vs boon-deadlock on a local file")
    p_bench.add_argument("file", type=Path, help="Path to a .dem or .dem.bz2 replay file")

    args = parser.parse_args(argv)

    if args.cmd == "build":
        bin_path = build_rust_binary()
        print(f"Built: {bin_path}")
        return 0

    if args.cmd == "harvest":
        _ensure_binary()
        summary = harvest_bulk(
            lake=args.data_dir,
            target_matches=args.matches,
            chunk_size=args.chunk_size,
            concurrency=args.concurrency,
            days_back=args.days_back,
            source=args.source,
            api_key=args.api_key,
        )
        print(json.dumps(summary, indent=2))
        return 0

    if args.cmd == "lookup":
        if not args.no_live_fetch:
            _ensure_binary()
        res = lookup_player_chats(
            lake=args.data_dir,
            username=args.username,
            exact=args.exact,
            live_fetch=not args.no_live_fetch,
            include_historical_names=args.historical_names,
            days_back=args.days_back,
            max_live_matches=args.max_live_matches,
            concurrency=args.concurrency,
            chat_type=args.chat_type,
            limit=args.limit,
            api_key=args.api_key,
        )
        df = res["messages"]
        print(f"\nFound {len(df)} chat message(s) for '{args.username}':")
        if not df.is_empty():
            display_cols = [
                c
                for c in ["match_id", "game_time_s", "player_name", "chat_type", "text"]
                if c in df.columns
            ]
            print(df.select(display_cols))
        return 0

    if args.cmd == "search":
        lake = ChatLake(args.data_dir)
        df = lake.search_messages(args.query, chat_type=args.chat_type, limit=args.limit)
        print(f"Found {len(df)} matching message(s):")
        if not df.is_empty():
            print(df.select(["match_id", "game_time_s", "player_name", "chat_type", "text"]))
        return 0

    if args.cmd == "stats":
        lake = ChatLake(args.data_dir)
        print(json.dumps(lake.stats(), indent=2))
        return 0

    if args.cmd == "compact":
        lake = ChatLake(args.data_dir)
        print(json.dumps(lake.compact(), indent=2))
        return 0

    if args.cmd == "benchmark":
        _ensure_binary()
        t0 = time.perf_counter()
        df_rust = extract_file_rust(args.file)
        t_rust = time.perf_counter() - t0
        print(f"Rust streaming extractor: {t_rust:.3f}s ({len(df_rust)} messages)")
        try:
            t1 = time.perf_counter()
            df_boon = extract_file_boon(args.file)
            t_boon = time.perf_counter() - t1
            print(f"boon-deadlock extractor:  {t_boon:.3f}s ({len(df_boon)} messages)")
            print(f"Speedup:                  {t_boon / max(t_rust, 1e-6):.1f}x faster")
        except ImportError:
            print("boon-deadlock not installed (install with `pip install boon-deadlock` to compare)")
        return 0

    return 1


if __name__ == "__main__":
    sys.exit(main())
