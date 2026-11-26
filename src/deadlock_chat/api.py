"""Deadlock API REST client and public Parquet Data Lake query helpers."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

import duckdb
import httpx

API_BASE_URL = "https://api.deadlock-api.com"
DATA_LAKE_MANIFEST_URL = "https://data.deadlock-api.com/v1/manifest.json"
DEFAULT_USER_AGENT = "deadlock-all-chat/0.1 (+https://github.com/veroiuxl/deadlock-all-chat)"

# Valid Deadlock match IDs are currently in the ~1e7–2e8 range; match_salts occasionally
# contains sentinel rows like 2147483648 (2^31) that always 404 on Valve's CDN.
MIN_VALID_MATCH_ID = 1_000_000
MAX_VALID_MATCH_ID = 1_000_000_000


def build_demo_url(cluster_id: int, match_id: int, replay_salt: int) -> str:
    """Construct Valve's Deadlock replay download URL."""
    return f"http://replay{cluster_id}.valve.net/1422450/{match_id}_{replay_salt}.dem.bz2"


@dataclass(frozen=True)
class MatchReplayJob:
    match_id: int
    demo_url: str
    cluster_id: int
    replay_salt: int
    start_time: int | None = None

    def to_job_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "match_id": int(self.match_id),
            "demo_url": self.demo_url,
        }
        if self.start_time is not None:
            d["start_time"] = int(self.start_time)
        return d


class DeadlockApiClient:
    """Client for https://api.deadlock-api.com and https://data.deadlock-api.com."""

    def __init__(
        self,
        api_key: str | None = None,
        timeout: float = 30.0,
        user_agent: str = DEFAULT_USER_AGENT,
    ) -> None:
        headers = {"User-Agent": user_agent}
        if api_key:
            headers["X-API-Key"] = api_key
        self._client = httpx.Client(headers=headers, timeout=timeout, follow_redirects=True)
        self._manifest_cache: dict[str, Any] | None = None
        self._manifest_ts: float = 0.0

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "DeadlockApiClient":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    def get_manifest(self, max_age_s: float = 1800.0) -> dict[str, Any]:
        """Fetch and cache the Deadlock API public Parquet Data Lake manifest."""
        now = time.time()
        if self._manifest_cache is not None and (now - self._manifest_ts) < max_age_s:
            return self._manifest_cache
        resp = self._client.get(DATA_LAKE_MANIFEST_URL)
        resp.raise_for_status()
        self._manifest_cache = resp.json()
        self._manifest_ts = now
        return self._manifest_cache

    def get_table_urls(self, table_name: str) -> list[str]:
        """Return full HTTPS Parquet URLs for a table in the Deadlock API Data Lake."""
        manifest = self.get_manifest()
        base = manifest["public_url"].rstrip("/")
        table = manifest["tables"].get(table_name)
        if not table:
            raise KeyError(f"Table '{table_name}' not found in data lake manifest")
        return [f"{base}/{f['key'].lstrip('/')}" for f in table["files"]]

    def _new_duckdb_httpfs(self) -> duckdb.DuckDBPyConnection:
        con = duckdb.connect()
        con.execute("INSTALL httpfs; LOAD httpfs;")
        return con

    def fetch_recent_replay_jobs_from_lake(
        self,
        days_back: int = 14,
        limit: int = 1000,
        exclude_match_ids: set[int] | None = None,
        newest_first: bool = True,
    ) -> list[MatchReplayJob]:
        """Query the public `match_salts` Parquet snapshot via DuckDB HTTP range requests.

        Returns up to `limit` valid matches with `replay_salt IS NOT NULL` created within
        `days_back` days (Valve retains Deadlock replays for ~14-21 days).
        """
        salt_urls = self.get_table_urls("match_salts")
        con = self._new_duckdb_httpfs()
        order = "DESC" if newest_first else "ASC"

        # Fetch extra rows if we need to filter out previously processed match_ids
        fetch_limit = limit + (len(exclude_match_ids) if exclude_match_ids else 0)
        # Cap query fetch to keep memory low when exclude set is very large
        if exclude_match_ids and len(exclude_match_ids) > 50_000:
            con.execute("CREATE TEMP TABLE _seen (match_id UBIGINT)")
            con.executemany(
                "INSERT INTO _seen VALUES (?)",
                [(int(mid),) for mid in exclude_match_ids],
            )
            query = f"""
            SELECT s.match_id, s.cluster_id, s.replay_salt, CAST(epoch(s.created_at) AS BIGINT)
            FROM read_parquet({salt_urls!r}) s
            LEFT ANTI JOIN _seen ON s.match_id = _seen.match_id
            WHERE s.replay_salt IS NOT NULL
              AND s.cluster_id IS NOT NULL
              AND s.match_id >= {MIN_VALID_MATCH_ID}
              AND s.match_id < {MAX_VALID_MATCH_ID}
              AND s.created_at >= current_timestamp - INTERVAL {int(days_back)} DAY
            ORDER BY s.match_id {order}
            LIMIT {int(limit)}
            """
            rows = con.execute(query).fetchall()
        else:
            query = f"""
            SELECT match_id, cluster_id, replay_salt, CAST(epoch(created_at) AS BIGINT)
            FROM read_parquet({salt_urls!r})
            WHERE replay_salt IS NOT NULL
              AND cluster_id IS NOT NULL
              AND match_id >= {MIN_VALID_MATCH_ID}
              AND match_id < {MAX_VALID_MATCH_ID}
              AND created_at >= current_timestamp - INTERVAL {int(days_back)} DAY
            ORDER BY match_id {order}
            LIMIT {int(min(fetch_limit, max(limit * 3, 5000)))}
            """
            rows = con.execute(query).fetchall()
            if exclude_match_ids:
                rows = [r for r in rows if int(r[0]) not in exclude_match_ids][:limit]
            else:
                rows = rows[:limit]

        con.close()
        return [
            MatchReplayJob(
                match_id=int(mid),
                cluster_id=int(cid),
                replay_salt=int(rsalt),
                demo_url=build_demo_url(int(cid), int(mid), int(rsalt)),
                start_time=int(ts) if ts is not None else None,
            )
            for mid, cid, rsalt, ts in rows
        ]

    def fetch_live_recent_matches(
        self,
        limit: int = 100,
        max_age_hours: float = 48.0,
        exclude_match_ids: set[int] | None = None,
    ) -> list[MatchReplayJob]:
        """Fetch newly ingested matches from `/v1/matches/recently-fetched` and resolve salts."""
        resp = self._client.get(f"{API_BASE_URL}/v1/matches/recently-fetched")
        resp.raise_for_status()
        matches = resp.json()
        now = time.time()
        matches = [
            m
            for m in matches
            if (now - m.get("start_time", 0)) <= max_age_hours * 3600
            and MIN_VALID_MATCH_ID <= m.get("match_id", 0) < MAX_VALID_MATCH_ID
            and (not exclude_match_ids or m["match_id"] not in exclude_match_ids)
        ]
        matches.sort(key=lambda m: m.get("start_time", 0), reverse=True)

        jobs: list[MatchReplayJob] = []
        for m in matches:
            if len(jobs) >= limit:
                break
            mid = int(m["match_id"])
            try:
                sresp = self._client.get(
                    f"{API_BASE_URL}/v1/matches/{mid}/salts",
                    params={"disable_steam": "true"},
                )
                if sresp.status_code != 200:
                    continue
                sdata = sresp.json()
                rsalt = sdata.get("replay_salt")
                cid = sdata.get("cluster_id")
                demo_url = sdata.get("demo_url")
                if rsalt and cid:
                    jobs.append(
                        MatchReplayJob(
                            match_id=mid,
                            cluster_id=int(cid),
                            replay_salt=int(rsalt),
                            demo_url=demo_url or build_demo_url(int(cid), mid, int(rsalt)),
                            start_time=int(m.get("start_time", 0)) or None,
                        )
                    )
            except httpx.HTTPError:
                continue
        return jobs

    def resolve_username(
        self,
        username: str,
        exact: bool = False,
        limit: int = 25,
        include_historical_lake: bool = False,
    ) -> list[dict[str, Any]]:
        """Resolve a Steam display name (or historical in-game name) to account_id(s)."""
        results: dict[int, dict[str, Any]] = {}

        # 1. Fast REST search via Deadlock API (/v1/players/steam-search)
        try:
            resp = self._client.get(
                f"{API_BASE_URL}/v1/players/steam-search",
                params={"search_query": username, "limit": limit},
            )
            if resp.status_code == 200:
                for item in resp.json():
                    aid = int(item["account_id"])
                    pname = item.get("personaname") or ""
                    if exact and pname.lower() != username.lower():
                        continue
                    results[aid] = {
                        "account_id": aid,
                        "personaname": pname,
                        "source": "steam_search",
                    }
        except httpx.HTTPError:
            pass

        # 2. Optional historical in-game name lookup via Data Lake `steam_profile_observed_names`
        if include_historical_lake:
            try:
                obs_urls = self.get_table_urls("steam_profile_observed_names")
                con = self._new_duckdb_httpfs()
                if exact:
                    q = f"""
                    SELECT DISTINCT account_id, observed_name
                    FROM read_parquet({obs_urls!r})
                    WHERE LOWER(observed_name) = LOWER(?)
                    LIMIT {int(limit)}
                    """
                    rows = con.execute(q, [username]).fetchall()
                else:
                    q = f"""
                    SELECT DISTINCT account_id, observed_name
                    FROM read_parquet({obs_urls!r})
                    WHERE observed_name ILIKE ?
                    LIMIT {int(limit)}
                    """
                    rows = con.execute(q, [f"%{username}%"]).fetchall()
                con.close()
                for aid, oname in rows:
                    aid_int = int(aid)
                    if aid_int not in results:
                        results[aid_int] = {
                            "account_id": aid_int,
                            "personaname": str(oname),
                            "source": "observed_names_lake",
                        }
            except Exception:
                pass

        return list(results.values())

    def get_player_recent_replay_jobs(
        self,
        account_id: int,
        days_back: int = 21,
        max_matches: int = 50,
        exclude_match_ids: set[int] | None = None,
    ) -> list[MatchReplayJob]:
        """Fetch a player's recent matches and resolve live Valve replay URLs."""
        resp = self._client.get(
            f"{API_BASE_URL}/v1/players/{int(account_id)}/match-history",
            params={"only_stored_history": "true"},
        )
        if resp.status_code != 200:
            return []
        history = resp.json()
        cutoff = time.time() - days_back * 86400

        candidate_matches: dict[int, int] = {}
        for m in history:
            mid = int(m["match_id"])
            st = int(m.get("start_time", 0))
            if st < cutoff:
                continue
            if not (MIN_VALID_MATCH_ID <= mid < MAX_VALID_MATCH_ID):
                continue
            if exclude_match_ids and mid in exclude_match_ids:
                continue
            candidate_matches[mid] = st

        if not candidate_matches:
            return []

        # Sort newest first and cap
        sorted_mids = sorted(
            candidate_matches.keys(),
            key=lambda m: candidate_matches[m],
            reverse=True,
        )[:max_matches]

        # Resolve salts via fast DB-backed REST endpoint (/v1/matches/{id}/salts?disable_steam=true)
        # Note: IP rate limit is 100 req/s for DB-backed lookups!
        jobs: list[MatchReplayJob] = []
        for mid in sorted_mids:
            try:
                sresp = self._client.get(
                    f"{API_BASE_URL}/v1/matches/{mid}/salts",
                    params={"disable_steam": "true"},
                )
                if sresp.status_code != 200:
                    continue
                sdata = sresp.json()
                rsalt = sdata.get("replay_salt")
                cid = sdata.get("cluster_id")
                demo_url = sdata.get("demo_url")
                if rsalt and cid:
                    jobs.append(
                        MatchReplayJob(
                            match_id=mid,
                            cluster_id=int(cid),
                            replay_salt=int(rsalt),
                            demo_url=demo_url or build_demo_url(int(cid), mid, int(rsalt)),
                            start_time=candidate_matches[mid],
                        )
                    )
            except httpx.HTTPError:
                continue
        return jobs
