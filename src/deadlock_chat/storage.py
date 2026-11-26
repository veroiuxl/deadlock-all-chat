"""Partitioned Parquet storage and DuckDB query engine for Deadlock chat messages."""

from __future__ import annotations

import shutil
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any

import duckdb
import polars as pl

from .extractor import CHAT_SCHEMA, empty_chats_df


def _stage_and_move_parquet(df: pl.DataFrame, dest: Path, compression_level: int = 6) -> Path:
    """Write Zstd Parquet to fast local /tmp first, then move to dest (Google Drive FUSE friendly)."""
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tf:
        tmp_path = Path(tf.name)
    try:
        df.write_parquet(tmp_path, compression="zstd", compression_level=compression_level)
        shutil.move(str(tmp_path), str(dest))
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise
    return dest


def _query_pl(
    con: duckdb.DuckDBPyConnection,
    query: str,
    params: list[Any] | None = None,
) -> pl.DataFrame:
    """Execute a DuckDB query and convert to a Polars DataFrame across DuckDB 1.x and 2.x."""
    if hasattr(con, "sql"):
        try:
            rel = con.sql(query, params=params) if params is not None else con.sql(query)
            if hasattr(rel, "to_arrow_table"):
                return pl.from_arrow(rel.to_arrow_table())
            if hasattr(rel, "pl"):
                return rel.pl()
        except TypeError:
            pass
    cur = con.execute(query, params) if params is not None else con.execute(query)
    if hasattr(cur, "fetch_arrow_table"):
        return pl.from_arrow(cur.fetch_arrow_table())
    if hasattr(cur, "to_arrow_table"):
        return pl.from_arrow(cur.to_arrow_table())
    if hasattr(cur, "pl"):
        return cur.pl()
    cols = [d[0] for d in cur.description]
    return pl.DataFrame(cur.fetchall(), schema=cols, orient="row")


class ChatLake:
    """Crash-safe Parquet data lake + DuckDB analytics/query layer.

    Layout (Google Drive friendly — append-only Parquet shards, zero locking):
      <root>/
        chats/part-<ts>-w<worker>-<id>.parquet   (Zstd-compressed chat message shards)
        state/proc-<ts>-w<worker>-<id>.parquet   (processed match checkpoint shards)
    """

    def __init__(self, root_dir: Path | str) -> None:
        self.root = Path(root_dir)
        self.chats_dir = self.root / "chats"
        self.state_dir = self.root / "state"
        self.chats_dir.mkdir(parents=True, exist_ok=True)
        self.state_dir.mkdir(parents=True, exist_ok=True)

    def _chat_files(self) -> list[str]:
        return sorted(str(p) for p in self.chats_dir.glob("*.parquet"))

    def _state_files(self) -> list[str]:
        return sorted(str(p) for p in self.state_dir.glob("*.parquet"))

    def append_chats(self, df: pl.DataFrame, worker_id: int = 0) -> Path | None:
        """Write a batch of extracted chat messages to an immutable Zstd Parquet shard."""
        if df.is_empty():
            return None
        ts = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
        shard = self.chats_dir / f"part-{ts}-w{int(worker_id)}-{uuid.uuid4().hex[:8]}.parquet"
        # Sort by (account_id, start_time, tick) so DuckDB row-group zone maps prune fast
        sorted_df = df.cast(CHAT_SCHEMA).sort(["account_id", "start_time", "tick"])
        return _stage_and_move_parquet(sorted_df, shard, compression_level=6)

    def append_status(self, status_df: pl.DataFrame, worker_id: int = 0) -> Path | None:
        """Persist processed match checkpoint records (enables seamless resume in Colab)."""
        if status_df.is_empty():
            return None
        ts = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
        shard = self.state_dir / f"proc-{ts}-w{int(worker_id)}-{uuid.uuid4().hex[:8]}.parquet"
        enriched = status_df.with_columns(pl.lit(int(time.time())).cast(pl.Int64).alias("processed_at"))
        return _stage_and_move_parquet(enriched, shard, compression_level=3)

    def get_processed_match_ids(self, include_errors: bool = False) -> set[int]:
        """Return all match IDs already processed (ok, no_chat, http_404)."""
        files = self._state_files()
        if not files:
            return set()
        con = duckdb.connect()
        where = "" if include_errors else "WHERE status IN ('ok', 'no_chat', 'http_404')"
        rows = con.execute(
            f"SELECT DISTINCT match_id FROM read_parquet({files!r}) {where}"
        ).fetchall()
        con.close()
        return {int(r[0]) for r in rows}

    def compact(self) -> dict[str, int]:
        """Merge many small shards into deduplicated, sorted Parquet files for optimal query speed."""
        summary = {"chat_shards_before": 0, "chat_rows": 0, "state_shards_before": 0, "state_rows": 0}

        chat_files = self._chat_files()
        summary["chat_shards_before"] = len(chat_files)
        if len(chat_files) > 1:
            con = duckdb.connect()
            ts = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
            tmp_out = self.chats_dir / f".compact-{ts}.parquet"
            final_out = self.chats_dir / f"compacted-{ts}-{uuid.uuid4().hex[:6]}.parquet"
            con.execute(
                f"""
                COPY (
                    SELECT DISTINCT *
                    FROM read_parquet({chat_files!r})
                    ORDER BY account_id, start_time, match_id, tick
                ) TO '{tmp_out}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)
                """
            )
            summary["chat_rows"] = int(
                con.execute(f"SELECT COUNT(*) FROM read_parquet('{tmp_out}')").fetchone()[0]
            )
            con.close()
            tmp_out.rename(final_out)
            for old in chat_files:
                Path(old).unlink(missing_ok=True)
        elif len(chat_files) == 1:
            con = duckdb.connect()
            summary["chat_rows"] = int(
                con.execute(f"SELECT COUNT(*) FROM read_parquet({chat_files!r})").fetchone()[0]
            )
            con.close()

        state_files = self._state_files()
        summary["state_shards_before"] = len(state_files)
        if len(state_files) > 1:
            con = duckdb.connect()
            ts = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
            tmp_out = self.state_dir / f".compact-{ts}.parquet"
            final_out = self.state_dir / f"compacted-{ts}-{uuid.uuid4().hex[:6]}.parquet"
            con.execute(
                f"""
                COPY (
                    SELECT match_id,
                           arg_max(status, processed_at) AS status,
                           arg_max(http_status, processed_at) AS http_status,
                           arg_max(bytes_downloaded, processed_at) AS bytes_downloaded,
                           arg_max(chat_count, processed_at) AS chat_count,
                           arg_max(player_count, processed_at) AS player_count,
                           arg_max(duration_ms, processed_at) AS duration_ms,
                           arg_max(error, processed_at) AS error,
                           MAX(processed_at) AS processed_at
                    FROM read_parquet({state_files!r})
                    GROUP BY match_id
                    ORDER BY match_id
                ) TO '{tmp_out}' (FORMAT PARQUET, COMPRESSION ZSTD)
                """
            )
            summary["state_rows"] = int(
                con.execute(f"SELECT COUNT(*) FROM read_parquet('{tmp_out}')").fetchone()[0]
            )
            con.close()
            tmp_out.rename(final_out)
            for old in state_files:
                Path(old).unlink(missing_ok=True)

        return summary

    def query_by_username(
        self,
        username: str,
        exact: bool = False,
        account_ids: list[int] | None = None,
        chat_type: str | None = None,
        limit: int = 500,
    ) -> pl.DataFrame:
        """Query all chat messages typed by a username (and/or resolved Steam account IDs)."""
        files = self._chat_files()
        if not files:
            return empty_chats_df()

        conditions: list[str] = []
        params: list[Any] = []

        name_conds: list[str] = []
        if exact:
            name_conds.append("LOWER(player_name) = LOWER(?)")
            params.append(username)
        else:
            name_conds.append("player_name ILIKE ?")
            params.append(f"%{username}%")

        if account_ids:
            placeholders = ", ".join("?" for _ in account_ids)
            name_conds.append(f"account_id IN ({placeholders})")
            params.extend(int(a) for a in account_ids)

        conditions.append("(" + " OR ".join(name_conds) + ")")

        if chat_type in ("all", "team"):
            conditions.append("chat_type = ?")
            params.append(chat_type)

        where_sql = " AND ".join(conditions)
        query = f"""
        SELECT DISTINCT *
        FROM read_parquet({files!r})
        WHERE {where_sql}
        ORDER BY start_time DESC, match_id DESC, tick ASC
        LIMIT {int(limit)}
        """
        con = duckdb.connect()
        result = _query_pl(con, query, params)
        con.close()
        return result

    def find_account_ids_by_username(
        self,
        username: str,
        exact: bool = False,
        limit: int = 25,
    ) -> list[dict[str, Any]]:
        """Resolve a username to account_id(s) already observed in the local Parquet lake."""
        files = self._chat_files()
        if not files:
            return []
        con = duckdb.connect()
        if exact:
            q = f"""
            SELECT DISTINCT account_id, player_name
            FROM read_parquet({files!r})
            WHERE account_id != 0 AND LOWER(player_name) = LOWER(?)
            LIMIT {int(limit)}
            """
            rows = con.execute(q, [username]).fetchall()
        else:
            q = f"""
            SELECT DISTINCT account_id, player_name
            FROM read_parquet({files!r})
            WHERE account_id != 0 AND player_name ILIKE ?
            LIMIT {int(limit)}
            """
            rows = con.execute(q, [f"%{username}%"]).fetchall()
        con.close()
        return [
            {"account_id": int(a), "personaname": str(n), "source": "local_lake"}
            for a, n in rows
        ]

    def query_by_account_id(
        self,
        account_id: int,
        chat_type: str | None = None,
        limit: int = 500,
    ) -> pl.DataFrame:
        """Query all chat messages by exact SteamID3 (`account_id`)."""
        files = self._chat_files()
        if not files:
            return empty_chats_df()

        params: list[Any] = [int(account_id)]
        extra = ""
        if chat_type in ("all", "team"):
            extra = " AND chat_type = ?"
            params.append(chat_type)

        query = f"""
        SELECT DISTINCT *
        FROM read_parquet({files!r})
        WHERE account_id = ? {extra}
        ORDER BY start_time DESC, match_id DESC, tick ASC
        LIMIT {int(limit)}
        """
        con = duckdb.connect()
        result = _query_pl(con, query, params)
        con.close()
        return result

    def search_messages(
        self,
        text_query: str,
        chat_type: str | None = None,
        limit: int = 200,
    ) -> pl.DataFrame:
        """Search collected chat messages by text substring (case-insensitive)."""
        files = self._chat_files()
        if not files:
            return empty_chats_df()

        params: list[Any] = [f"%{text_query}%"]
        extra = ""
        if chat_type in ("all", "team"):
            extra = " AND chat_type = ?"
            params.append(chat_type)

        query = f"""
        SELECT DISTINCT *
        FROM read_parquet({files!r})
        WHERE text ILIKE ? {extra}
        ORDER BY start_time DESC, match_id DESC, tick ASC
        LIMIT {int(limit)}
        """
        con = duckdb.connect()
        result = _query_pl(con, query, params)
        con.close()
        return result

    def sql(self, query: str) -> pl.DataFrame:
        """Run an arbitrary DuckDB SQL query with `chats` and `processed` views registered."""
        con = duckdb.connect()
        chat_files = self._chat_files()
        state_files = self._state_files()
        if chat_files:
            con.execute(f"CREATE VIEW chats AS SELECT * FROM read_parquet({chat_files!r})")
        if state_files:
            con.execute(f"CREATE VIEW processed AS SELECT * FROM read_parquet({state_files!r})")
        result = _query_pl(con, query)
        con.close()
        return result

    def stats(self) -> dict[str, Any]:
        """Compute high-level statistics for the local chat Parquet lake."""
        chat_files = self._chat_files()
        state_files = self._state_files()
        chat_bytes = sum(Path(f).stat().st_size for f in chat_files)
        state_bytes = sum(Path(f).stat().st_size for f in state_files)

        out: dict[str, Any] = {
            "root_dir": str(self.root),
            "chat_shards": len(chat_files),
            "chat_storage_mb": round(chat_bytes / 1e6, 3),
            "state_shards": len(state_files),
            "state_storage_mb": round(state_bytes / 1e6, 3),
            "total_messages": 0,
            "unique_matches_with_chat": 0,
            "unique_players": 0,
            "all_chat_messages": 0,
            "team_chat_messages": 0,
            "matches_by_status": {},
            "top_players": [],
        }

        con = duckdb.connect()
        if chat_files:
            row = con.execute(
                f"""
                SELECT
                    COUNT(*),
                    COUNT(DISTINCT match_id),
                    COUNT(DISTINCT account_id),
                    COUNT(*) FILTER (WHERE chat_type = 'all'),
                    COUNT(*) FILTER (WHERE chat_type = 'team')
                FROM read_parquet({chat_files!r})
                """
            ).fetchone()
            out["total_messages"] = int(row[0])
            out["unique_matches_with_chat"] = int(row[1])
            out["unique_players"] = int(row[2])
            out["all_chat_messages"] = int(row[3])
            out["team_chat_messages"] = int(row[4])

            top = con.execute(
                f"""
                SELECT account_id, ANY_VALUE(player_name) AS name, COUNT(*) AS msg_count
                FROM read_parquet({chat_files!r})
                GROUP BY account_id
                ORDER BY msg_count DESC
                LIMIT 10
                """
            ).fetchall()
            out["top_players"] = [
                {"account_id": int(a), "player_name": str(n), "messages": int(c)}
                for a, n, c in top
            ]

        if state_files:
            srows = con.execute(
                f"""
                SELECT status, COUNT(DISTINCT match_id)
                FROM read_parquet({state_files!r})
                GROUP BY status
                ORDER BY 2 DESC
                """
            ).fetchall()
            out["matches_by_status"] = {str(s): int(c) for s, c in srows}

        con.close()
        return out
