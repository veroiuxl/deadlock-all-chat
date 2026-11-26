"""Tests for deadlock-all-chat storage, query engine, and Rust vs Boon parity."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from deadlock_chat.api import build_demo_url
from deadlock_chat.extractor import CHAT_SCHEMA, STATUS_SCHEMA, extract_file_boon, extract_file_rust, find_rust_binary
from deadlock_chat.storage import ChatLake


def _sample_chats() -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "match_id": 100000001,
                "start_time": 1795000000,
                "duration_s": 1800,
                "game_mode": 1,
                "match_mode": 1,
                "tick": 6400,
                "game_time_s": 100.0,
                "player_slot": 0,
                "account_id": 123456,
                "steam_id": 76561197960389184,
                "player_name": "AlphaPlayer",
                "hero_id": 1,
                "team": 0,
                "chat_type": "all",
                "text": "gl hf everyone",
            },
            {
                "match_id": 100000001,
                "start_time": 1795000000,
                "duration_s": 1800,
                "game_mode": 1,
                "match_mode": 1,
                "tick": 12800,
                "game_time_s": 200.0,
                "player_slot": 0,
                "account_id": 123456,
                "steam_id": 76561197960389184,
                "player_name": "AlphaPlayer",
                "hero_id": 1,
                "team": 0,
                "chat_type": "team",
                "text": "push mid boss now",
            },
            {
                "match_id": 100000002,
                "start_time": 1795010000,
                "duration_s": 2100,
                "game_mode": 1,
                "match_mode": 1,
                "tick": 3200,
                "game_time_s": 50.0,
                "player_slot": 3,
                "account_id": 123456,
                "steam_id": 76561197960389184,
                "player_name": "RenamedAlpha",
                "hero_id": 14,
                "team": 1,
                "chat_type": "all",
                "text": "gg wp!",
            },
            {
                "match_id": 100000002,
                "start_time": 1795010000,
                "duration_s": 2100,
                "game_mode": 1,
                "match_mode": 1,
                "tick": 4000,
                "game_time_s": 62.5,
                "player_slot": 4,
                "account_id": 999999,
                "steam_id": 76561197961265727,
                "player_name": "OtherGamer",
                "hero_id": 22,
                "team": 0,
                "chat_type": "all",
                "text": "nice hook",
            },
        ],
        schema=CHAT_SCHEMA,
    )


def test_build_demo_url() -> None:
    assert (
        build_demo_url(142, 144006353, 1794999083)
        == "http://replay142.valve.net/1422450/144006353_1794999083.dem.bz2"
    )


def test_chat_lake_append_query_and_compact(tmp_path: Path) -> None:
    lake = ChatLake(tmp_path / "lake")
    df = _sample_chats()

    # Append across two shards from different workers (including a duplicate row to test deduplication)
    s0 = lake.append_chats(df.slice(0, 2), worker_id=0)
    s1 = lake.append_chats(df.slice(1, 3), worker_id=1)
    assert s0 is not None and "-w0-" in s0.name
    assert s1 is not None and "-w1-" in s1.name

    # Substring query by username
    res_sub = lake.query_by_username("alpha")
    assert len(res_sub) == 3

    # Exact username query
    res_exact = lake.query_by_username("AlphaPlayer", exact=True)
    assert len(res_exact) == 2

    # Exact username + resolved account_id catches messages under a previous/renamed handle!
    res_by_id = lake.query_by_username("AlphaPlayer", exact=True, account_ids=[123456])
    assert len(res_by_id) == 3
    assert set(res_by_id["player_name"].to_list()) == {"AlphaPlayer", "RenamedAlpha"}

    # Chat type filter
    res_team = lake.query_by_username("AlphaPlayer", chat_type="team")
    assert len(res_team) == 1
    assert res_team["text"][0] == "push mid boss now"

    # Full-text keyword search
    res_kw = lake.search_messages("mid boss")
    assert len(res_kw) == 1
    assert res_kw["account_id"][0] == 123456

    # Checkpoint status tracking
    status_df = pl.DataFrame(
        [
            {
                "match_id": 100000001,
                "status": "ok",
                "http_status": 200,
                "bytes_downloaded": 35000000,
                "chat_count": 2,
                "player_count": 12,
                "duration_ms": 1200,
                "error": None,
            },
            {
                "match_id": 100000003,
                "status": "http_404",
                "http_status": 404,
                "bytes_downloaded": 0,
                "chat_count": 0,
                "player_count": 0,
                "duration_ms": 120,
                "error": None,
            },
            {
                "match_id": 100000004,
                "status": "error",
                "http_status": 503,
                "bytes_downloaded": 0,
                "chat_count": 0,
                "player_count": 0,
                "duration_ms": 5000,
                "error": "HTTP 503",
            },
        ],
        schema=STATUS_SCHEMA,
    )
    lake.append_status(status_df)
    seen = lake.get_processed_match_ids(include_errors=False)
    assert seen == {100000001, 100000003}
    assert 100000004 in lake.get_processed_match_ids(include_errors=True)

    # Compact shards and re-verify
    comp = lake.compact()
    assert comp["chat_shards_before"] == 2
    assert comp["chat_rows"] == 4
    stats = lake.stats()
    assert stats["total_messages"] == 4
    assert stats["unique_players"] == 2
    assert stats["all_chat_messages"] == 3
    assert stats["team_chat_messages"] == 1


@pytest.mark.skipif(
    not Path("/tmp/test_144006353.dem.bz2").is_file() or find_rust_binary() is None,
    reason="Requires local test replay and compiled Rust binary",
)
def test_rust_matches_boon_on_real_replay() -> None:
    path = Path("/tmp/test_144006353.dem.bz2")
    df_rust = extract_file_rust(path)
    df_boon = extract_file_boon(path)

    assert len(df_rust) == len(df_boon)
    assert df_rust["tick"].to_list() == df_boon["tick"].to_list()
    assert df_rust["account_id"].to_list() == df_boon["account_id"].to_list()
    assert df_rust["player_name"].to_list() == df_boon["player_name"].to_list()
    assert df_rust["hero_id"].to_list() == df_boon["hero_id"].to_list()
    assert df_rust["chat_type"].to_list() == df_boon["chat_type"].to_list()
    assert df_rust["text"].to_list() == df_boon["text"].to_list()


def test_fetch_recent_replay_jobs_exclude_and_sharding(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from deadlock_chat.api import DeadlockApiClient

    fake_salts = tmp_path / "salts.parquet"
    pl.DataFrame(
        {
            "match_id": [10_000_001, 10_000_002, 10_000_003, 10_000_004, 10_000_005],
            "cluster_id": [142, 143, 142, 144, 145],
            "replay_salt": [111, 222, 333, 444, 555],
            "created_ts": [1795000001, 1795000002, 1795000003, 1795000004, 1795000005],
        }
    ).write_parquet(fake_salts)

    with DeadlockApiClient() as api:
        monkeypatch.setattr(api, "_get_cached_salts_parquet", lambda days_back, cache_dir=None: str(fake_salts))

        jobs = api.fetch_recent_replay_jobs_from_lake(
            days_back=5,
            limit=10,
            exclude_match_ids={10_000_005, 10_000_002},
        )
        assert [j.match_id for j in jobs] == [10_000_004, 10_000_003, 10_000_001]

        w0 = api.fetch_recent_replay_jobs_from_lake(
            days_back=5, limit=10, exclude_match_ids={10_000_002}, worker_id=0, num_workers=2
        )
        w1 = api.fetch_recent_replay_jobs_from_lake(
            days_back=5, limit=10, exclude_match_ids={10_000_002}, worker_id=1, num_workers=2
        )
        assert [j.match_id for j in w0] == [10_000_004]
        assert [j.match_id for j in w1] == [10_000_005, 10_000_003, 10_000_001]

