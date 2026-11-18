"""Public interface for the Bluesky reader and post work queue.

Only this module owns the post queue schema.  Reader and detector processes may
each create an adapter instance, but they should not issue SQL directly.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from hashlib import sha256
import json
from pathlib import Path
import shutil
import sqlite3
import time
from typing import Iterator, Sequence
from uuid import uuid4


POST_COLLECTION = "app.bsky.feed.post"


@dataclass(frozen=True, slots=True)
class PostJob:
    job_id: str
    stream_id: str
    sequence: int
    operation_index: int
    post_uri: str
    post_cid: str
    author_did: str
    text: str
    created_at: str | None
    received_at: float

    @classmethod
    def create(
        cls,
        *,
        stream_id: str,
        sequence: int,
        operation_index: int,
        post_uri: str,
        post_cid: str,
        author_did: str,
        text: str,
        created_at: str | None = None,
        received_at: float | None = None,
    ) -> "PostJob":
        identity = f"{stream_id}\0{sequence}\0{operation_index}".encode()
        return cls(
            job_id=sha256(identity).hexdigest(),
            stream_id=stream_id,
            sequence=sequence,
            operation_index=operation_index,
            post_uri=post_uri,
            post_cid=post_cid,
            author_did=author_did,
            text=text,
            created_at=created_at,
            received_at=time.time() if received_at is None else received_at,
        )


@dataclass(frozen=True, slots=True)
class ClaimedPost:
    job: PostJob
    attempt_count: int
    lease_until: float


class PostQueue:
    """SQLite-backed competing-consumer queue for new Bluesky posts."""

    _METRIC_COLUMNS = {
        "ingested",
        "detected_positive",
        "detected_negative",
        "retried",
        "dead_lettered",
    }

    def __init__(
        self,
        database: str | Path,
        *,
        busy_timeout: float = 5.0,
        migrate: bool = True,
    ):
        self.database = Path(database)
        self.database.parent.mkdir(parents=True, exist_ok=True)
        self.busy_timeout = busy_timeout
        if migrate:
            self._migrate()

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(
            self.database,
            timeout=self.busy_timeout,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        connection.execute(f"PRAGMA busy_timeout = {int(self.busy_timeout * 1000)}")
        connection.execute("PRAGMA foreign_keys = ON")
        try:
            yield connection
        finally:
            connection.close()

    def _migrate(self) -> None:
        with self._connect() as connection:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = NORMAL")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS post_jobs (
                    job_id TEXT PRIMARY KEY,
                    stream_id TEXT NOT NULL,
                    sequence INTEGER NOT NULL,
                    operation_index INTEGER NOT NULL,
                    post_uri TEXT NOT NULL,
                    post_cid TEXT NOT NULL,
                    author_did TEXT NOT NULL,
                    text TEXT NOT NULL,
                    created_at TEXT,
                    received_at REAL NOT NULL,
                    enqueued_at REAL NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending', 'leased', 'dead')),
                    available_at REAL NOT NULL,
                    lease_until REAL,
                    leased_by TEXT,
                    attempt_count INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT
                );

                CREATE INDEX IF NOT EXISTS post_jobs_claimable
                    ON post_jobs(status, available_at, lease_until, sequence);

                CREATE INDEX IF NOT EXISTS post_jobs_active_age
                    ON post_jobs(enqueued_at) WHERE status != 'dead';

                CREATE TABLE IF NOT EXISTS stream_cursors (
                    stream_id TEXT PRIMARY KEY,
                    sequence INTEGER NOT NULL,
                    updated_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS stream_health (
                    stream_id TEXT PRIMARY KEY,
                    connected INTEGER NOT NULL DEFAULT 0,
                    reconnect_count INTEGER NOT NULL DEFAULT 0,
                    received_total INTEGER NOT NULL DEFAULT 0,
                    invalid_total INTEGER NOT NULL DEFAULT 0,
                    last_event_at REAL,
                    source_event_at REAL,
                    updated_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS post_queue_totals (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    ingested INTEGER NOT NULL DEFAULT 0,
                    acknowledged INTEGER NOT NULL DEFAULT 0,
                    retried INTEGER NOT NULL DEFAULT 0,
                    dead_lettered INTEGER NOT NULL DEFAULT 0
                );

                INSERT OR IGNORE INTO post_queue_totals(singleton) VALUES (1);

                CREATE TABLE IF NOT EXISTS post_metric_buckets (
                    bucket INTEGER PRIMARY KEY,
                    ingested INTEGER NOT NULL DEFAULT 0,
                    detected_positive INTEGER NOT NULL DEFAULT 0,
                    detected_negative INTEGER NOT NULL DEFAULT 0,
                    retried INTEGER NOT NULL DEFAULT 0,
                    dead_lettered INTEGER NOT NULL DEFAULT 0,
                    claimed INTEGER NOT NULL DEFAULT 0,
                    claim_wait_seconds REAL NOT NULL DEFAULT 0,
                    claim_transactions INTEGER NOT NULL DEFAULT 0,
                    claim_db_seconds REAL NOT NULL DEFAULT 0,
                    completed INTEGER NOT NULL DEFAULT 0,
                    traversal_seconds REAL NOT NULL DEFAULT 0
                );
                """
            )
            stream_health_columns = {
                str(row["name"])
                for row in connection.execute("PRAGMA table_info(stream_health)")
            }
            if "source_event_at" not in stream_health_columns:
                connection.execute("ALTER TABLE stream_health ADD COLUMN source_event_at REAL")
            metric_columns = {
                str(row["name"])
                for row in connection.execute("PRAGMA table_info(post_metric_buckets)")
            }
            for column, definition in (
                ("claimed", "INTEGER NOT NULL DEFAULT 0"),
                ("claim_wait_seconds", "REAL NOT NULL DEFAULT 0"),
                ("claim_transactions", "INTEGER NOT NULL DEFAULT 0"),
                ("claim_db_seconds", "REAL NOT NULL DEFAULT 0"),
                ("completed", "INTEGER NOT NULL DEFAULT 0"),
                ("traversal_seconds", "REAL NOT NULL DEFAULT 0"),
            ):
                if column not in metric_columns:
                    connection.execute(
                        f"ALTER TABLE post_metric_buckets ADD COLUMN {column} {definition}"
                    )

    @staticmethod
    def _begin(connection: sqlite3.Connection) -> None:
        connection.execute("BEGIN IMMEDIATE")

    @classmethod
    def _increment_metric(
        cls,
        connection: sqlite3.Connection,
        metric: str,
        amount: int,
        now: float,
    ) -> None:
        if metric not in cls._METRIC_COLUMNS:
            raise ValueError(f"unsupported metric: {metric}")
        bucket = int(now // 60) * 60
        connection.execute(
            f"""
            INSERT INTO post_metric_buckets(bucket, {metric}) VALUES (?, ?)
            ON CONFLICT(bucket) DO UPDATE
                SET {metric} = {metric} + excluded.{metric}
            """,
            (bucket, amount),
        )

    def enqueue_and_checkpoint(
        self,
        jobs: Sequence[PostJob],
        *,
        stream_id: str,
        sequence: int,
        source_event_at: float | None = None,
        now: float | None = None,
    ) -> int:
        """Durably insert jobs and advance the stream cursor atomically."""
        timestamp = time.time() if now is None else now
        if any(job.stream_id != stream_id or job.sequence != sequence for job in jobs):
            raise ValueError("all jobs must belong to the checkpointed stream event")

        inserted = 0
        with self._connect() as connection:
            self._begin(connection)
            try:
                current = connection.execute(
                    "SELECT sequence FROM stream_cursors WHERE stream_id = ?",
                    (stream_id,),
                ).fetchone()
                if current is not None and int(current["sequence"]) >= sequence:
                    connection.rollback()
                    return 0

                for job in jobs:
                    cursor = connection.execute(
                        """
                        INSERT OR IGNORE INTO post_jobs(
                            job_id, stream_id, sequence, operation_index,
                            post_uri, post_cid, author_did, text, created_at,
                            received_at, enqueued_at, available_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            job.job_id,
                            job.stream_id,
                            job.sequence,
                            job.operation_index,
                            job.post_uri,
                            job.post_cid,
                            job.author_did,
                            job.text,
                            job.created_at,
                            job.received_at,
                            timestamp,
                            timestamp,
                        ),
                    )
                    inserted += cursor.rowcount

                connection.execute(
                    """
                    INSERT INTO stream_cursors(stream_id, sequence, updated_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT(stream_id) DO UPDATE SET
                        sequence = excluded.sequence,
                        updated_at = excluded.updated_at
                    """,
                    (stream_id, sequence, timestamp),
                )
                connection.execute(
                    """
                    INSERT INTO stream_health(
                        stream_id, connected, received_total, last_event_at,
                        source_event_at, updated_at
                    ) VALUES (?, 1, 1, ?, ?, ?)
                    ON CONFLICT(stream_id) DO UPDATE SET
                        connected = 1,
                        received_total = stream_health.received_total + 1,
                        last_event_at = excluded.last_event_at,
                        source_event_at = COALESCE(
                            excluded.source_event_at,
                            stream_health.source_event_at
                        ),
                        updated_at = excluded.updated_at
                    """,
                    (stream_id, timestamp, source_event_at, timestamp),
                )
                if inserted:
                    connection.execute(
                        "UPDATE post_queue_totals SET ingested = ingested + ? WHERE singleton = 1",
                        (inserted,),
                    )
                    self._increment_metric(connection, "ingested", inserted, timestamp)
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
        return inserted

    def get_cursor(self, stream_id: str) -> int | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT sequence FROM stream_cursors WHERE stream_id = ?",
                (stream_id,),
            ).fetchone()
        return None if row is None else int(row["sequence"])

    def set_stream_connected(
        self,
        stream_id: str,
        connected: bool,
        *,
        reconnect: bool = False,
        now: float | None = None,
    ) -> None:
        timestamp = time.time() if now is None else now
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO stream_health(
                    stream_id, connected, reconnect_count, updated_at
                ) VALUES (?, ?, ?, ?)
                ON CONFLICT(stream_id) DO UPDATE SET
                    connected = excluded.connected,
                    reconnect_count = stream_health.reconnect_count + excluded.reconnect_count,
                    updated_at = excluded.updated_at
                """,
                (stream_id, int(connected), int(reconnect), timestamp),
            )

    def record_invalid_event(self, stream_id: str, *, now: float | None = None) -> None:
        timestamp = time.time() if now is None else now
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO stream_health(stream_id, invalid_total, updated_at)
                VALUES (?, 1, ?)
                ON CONFLICT(stream_id) DO UPDATE SET
                    invalid_total = stream_health.invalid_total + 1,
                    updated_at = excluded.updated_at
                """,
                (stream_id, timestamp),
            )

    def claim(
        self,
        worker_id: str,
        *,
        limit: int = 1,
        lease_seconds: float = 60.0,
        now: float | None = None,
    ) -> list[ClaimedPost]:
        if limit < 1:
            raise ValueError("limit must be positive")
        timestamp = time.time() if now is None else now
        lease_until = timestamp + lease_seconds
        claim_started = time.perf_counter()

        with self._connect() as connection:
            self._begin(connection)
            try:
                rows = connection.execute(
                    """
                    SELECT * FROM post_jobs
                    WHERE status != 'dead'
                      AND available_at <= ?
                      AND (status = 'pending' OR lease_until <= ?)
                    ORDER BY sequence, operation_index
                    LIMIT ?
                    """,
                    (timestamp, timestamp, limit),
                ).fetchall()
                job_ids = [row["job_id"] for row in rows]
                if job_ids:
                    placeholders = ",".join("?" for _ in job_ids)
                    connection.execute(
                        f"""
                        UPDATE post_jobs
                        SET status = 'leased', leased_by = ?, lease_until = ?,
                            attempt_count = attempt_count + 1
                        WHERE job_id IN ({placeholders})
                        """,
                        (worker_id, lease_until, *job_ids),
                    )
                    rows = connection.execute(
                        f"SELECT * FROM post_jobs WHERE job_id IN ({placeholders}) "
                        "ORDER BY sequence, operation_index",
                        job_ids,
                    ).fetchall()
                    wait_seconds = sum(
                        max(0.0, timestamp - float(row["enqueued_at"])) for row in rows
                    )
                    claim_seconds = time.perf_counter() - claim_started
                    bucket = int(timestamp // 60) * 60
                    connection.execute(
                        """
                        INSERT INTO post_metric_buckets(
                            bucket, claimed, claim_wait_seconds,
                            claim_transactions, claim_db_seconds
                        ) VALUES (?, ?, ?, 1, ?)
                        ON CONFLICT(bucket) DO UPDATE SET
                            claimed = claimed + excluded.claimed,
                            claim_wait_seconds = claim_wait_seconds
                                + excluded.claim_wait_seconds,
                            claim_transactions = claim_transactions + 1,
                            claim_db_seconds = claim_db_seconds
                                + excluded.claim_db_seconds
                        """,
                        (bucket, len(rows), wait_seconds, claim_seconds),
                    )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

        return [
            ClaimedPost(
                job=self._row_to_job(row),
                attempt_count=int(row["attempt_count"]),
                lease_until=float(row["lease_until"]),
            )
            for row in rows
        ]

    def acknowledge(
        self,
        job_id: str,
        worker_id: str,
        *,
        outcome: str,
        now: float | None = None,
    ) -> bool:
        if outcome not in {"positive", "negative"}:
            raise ValueError("outcome must be 'positive' or 'negative'")
        timestamp = time.time() if now is None else now
        with self._connect() as connection:
            self._begin(connection)
            try:
                row = connection.execute(
                    """
                    SELECT received_at FROM post_jobs
                    WHERE job_id = ? AND status = 'leased' AND leased_by = ?
                    """,
                    (job_id, worker_id),
                ).fetchone()
                acknowledged = row is not None
                if acknowledged:
                    connection.execute("DELETE FROM post_jobs WHERE job_id = ?", (job_id,))
                    connection.execute(
                        "UPDATE post_queue_totals SET acknowledged = acknowledged + 1 WHERE singleton = 1"
                    )
                    self._increment_metric(
                        connection,
                        f"detected_{outcome}",
                        1,
                        timestamp,
                    )
                    bucket = int(timestamp // 60) * 60
                    connection.execute(
                        """
                        INSERT INTO post_metric_buckets(
                            bucket, completed, traversal_seconds
                        ) VALUES (?, 1, ?)
                        ON CONFLICT(bucket) DO UPDATE SET
                            completed = completed + 1,
                            traversal_seconds = traversal_seconds
                                + excluded.traversal_seconds
                        """,
                        (bucket, max(0.0, timestamp - float(row["received_at"]))),
                    )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
        return acknowledged

    def retry(
        self,
        job_id: str,
        worker_id: str,
        error: str,
        *,
        delay_seconds: float = 5.0,
        max_attempts: int = 5,
        now: float | None = None,
    ) -> bool:
        timestamp = time.time() if now is None else now
        with self._connect() as connection:
            self._begin(connection)
            try:
                row = connection.execute(
                    """
                    SELECT attempt_count FROM post_jobs
                    WHERE job_id = ? AND status = 'leased' AND leased_by = ?
                    """,
                    (job_id, worker_id),
                ).fetchone()
                if row is None:
                    connection.rollback()
                    return False

                dead = int(row["attempt_count"]) >= max_attempts
                connection.execute(
                    """
                    UPDATE post_jobs SET status = ?, available_at = ?,
                        lease_until = NULL, leased_by = NULL, last_error = ?
                    WHERE job_id = ?
                    """,
                    (
                        "dead" if dead else "pending",
                        timestamp if dead else timestamp + delay_seconds,
                        error[:2000],
                        job_id,
                    ),
                )
                total_column = "dead_lettered" if dead else "retried"
                connection.execute(
                    f"UPDATE post_queue_totals SET {total_column} = {total_column} + 1 "
                    "WHERE singleton = 1"
                )
                self._increment_metric(
                    connection,
                    "dead_lettered" if dead else "retried",
                    1,
                    timestamp,
                )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
        return True

    def metrics(
        self,
        *,
        window_seconds: int = 60,
        now: float | None = None,
    ) -> dict[str, float | int | bool | str | None]:
        timestamp = time.time() if now is None else now
        first_bucket = int((timestamp - window_seconds) // 60) * 60
        with self._connect() as connection:
            connection.execute("BEGIN")
            totals = connection.execute(
                "SELECT * FROM post_queue_totals WHERE singleton = 1"
            ).fetchone()
            leased_row = connection.execute(
                """
                SELECT COUNT(*) AS leased,
                       COUNT(DISTINCT leased_by) AS busy_workers
                FROM post_jobs
                WHERE status = 'leased' AND lease_until > ?
                """,
                (timestamp,),
            ).fetchone()
            oldest_row = connection.execute(
                """
                SELECT MIN(enqueued_at) AS oldest
                FROM post_jobs WHERE status != 'dead'
                """
            ).fetchone()
            rates = connection.execute(
                """
                SELECT COALESCE(SUM(ingested), 0) AS ingested,
                       COALESCE(SUM(detected_positive), 0) AS positive,
                       COALESCE(SUM(detected_negative), 0) AS negative,
                       COALESCE(SUM(retried), 0) AS retried,
                       COALESCE(SUM(claimed), 0) AS claimed,
                       COALESCE(SUM(claim_wait_seconds), 0) AS claim_wait_seconds,
                       COALESCE(SUM(claim_transactions), 0) AS claim_transactions,
                       COALESCE(SUM(claim_db_seconds), 0) AS claim_db_seconds,
                       COALESCE(SUM(completed), 0) AS completed,
                       COALESCE(SUM(traversal_seconds), 0) AS traversal_seconds
                FROM post_metric_buckets WHERE bucket >= ?
                """,
                (first_bucket,),
            ).fetchone()
            stream = connection.execute(
                "SELECT * FROM stream_health ORDER BY updated_at DESC LIMIT 1"
            ).fetchone()
            connection.commit()

        dead = int(totals["dead_lettered"])
        depth = max(0, int(totals["ingested"]) - int(totals["acknowledged"]) - dead)
        leased = min(depth, int(leased_row["leased"] or 0))
        claimed = int(rates["claimed"] or 0)
        claim_transactions = int(rates["claim_transactions"] or 0)
        completed = int(rates["completed"] or 0)
        oldest = oldest_row["oldest"]
        result: dict[str, float | int | bool | str | None] = {
            "available": depth - leased,
            "leased": leased,
            "busy_workers": int(leased_row["busy_workers"] or 0),
            "dead": dead,
            "oldest_job_age_seconds": None if oldest is None else max(0.0, timestamp - float(oldest)),
            "ingestion_rate_per_second": float(rates["ingested"]) / window_seconds,
            "label_rate_per_second": float(rates["positive"]) / window_seconds,
            "detection_rate_per_second": (float(rates["positive"]) + float(rates["negative"])) / window_seconds,
            "retry_rate_per_second": float(rates["retried"]) / window_seconds,
            "average_queue_wait_seconds": (
                None
                if claimed == 0
                else float(rates["claim_wait_seconds"]) / claimed
            ),
            "average_claim_seconds": (
                None
                if claim_transactions == 0
                else float(rates["claim_db_seconds"]) / claim_transactions
            ),
            "average_traversal_seconds": (
                None
                if completed == 0
                else float(rates["traversal_seconds"]) / completed
            ),
            "recent_claimed": claimed,
            "recent_claim_wait_seconds": float(rates["claim_wait_seconds"]),
            "recent_claim_transactions": claim_transactions,
            "recent_claim_db_seconds": float(rates["claim_db_seconds"]),
            "recent_completed": completed,
            "recent_traversal_seconds": float(rates["traversal_seconds"]),
            "ingested_total": int(totals["ingested"]),
            "acknowledged_total": int(totals["acknowledged"]),
            "retried_total": int(totals["retried"]),
            "dead_lettered_total": int(totals["dead_lettered"]),
        }
        if stream is not None:
            last_event_at = stream["last_event_at"]
            source_event_at = stream["source_event_at"]
            result.update(
                {
                    "stream_id": str(stream["stream_id"]),
                    "stream_connected": bool(stream["connected"]),
                    "stream_reconnect_total": int(stream["reconnect_count"]),
                    "stream_received_total": int(stream["received_total"]),
                    "stream_invalid_total": int(stream["invalid_total"]),
                    "stream_last_event_age_seconds": (
                        None
                        if last_event_at is None
                        else max(0.0, timestamp - float(last_event_at))
                    ),
                    "stream_source_lag_seconds": (
                        None
                        if source_event_at is None
                        else max(0.0, timestamp - float(source_event_at))
                    ),
                }
            )
        else:
            result.update(
                {
                    "stream_id": None,
                    "stream_connected": False,
                    "stream_reconnect_total": 0,
                    "stream_received_total": 0,
                    "stream_invalid_total": 0,
                    "stream_last_event_age_seconds": None,
                    "stream_source_lag_seconds": None,
                }
            )
        return result

    @staticmethod
    def _row_to_job(row: sqlite3.Row) -> PostJob:
        return PostJob(
            job_id=row["job_id"],
            stream_id=row["stream_id"],
            sequence=int(row["sequence"]),
            operation_index=int(row["operation_index"]),
            post_uri=row["post_uri"],
            post_cid=row["post_cid"],
            author_did=row["author_did"],
            text=row["text"],
            created_at=row["created_at"],
            received_at=float(row["received_at"]),
        )


def _shard_manifest_path(database: Path) -> Path:
    return database.with_name(f"{database.name}.shards.json")


def _shard_root(database: Path) -> Path:
    return database.with_name(f"{database.name}.shards")


def _load_shard_manifest(database: Path) -> dict[str, object] | None:
    manifest_path = _shard_manifest_path(database)
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    if not isinstance(payload, dict) or payload.get("version") != 1:
        raise RuntimeError(f"invalid post-shard manifest: {manifest_path}")
    raw_paths = payload.get("paths")
    if not isinstance(raw_paths, list) or not all(
        isinstance(path, str) for path in raw_paths
    ):
        raise RuntimeError(f"invalid post-shard paths in manifest: {manifest_path}")
    paths = [database.parent / str(path) for path in raw_paths]
    if not paths or any(not path.is_file() for path in paths):
        raise RuntimeError(f"post-shard manifest references missing databases: {manifest_path}")
    return {**payload, "resolved_paths": paths}


def configured_post_shard_paths(database: str | Path, shard_count: int) -> list[Path]:
    """Return active shard paths, or deterministic placeholders before rectification."""
    if shard_count < 1:
        raise ValueError("post shard count must be positive")
    base = Path(database)
    manifest = _load_shard_manifest(base)
    if manifest is not None and int(manifest.get("shard_count", 0)) == shard_count:
        return list(manifest["resolved_paths"])  # type: ignore[arg-type]
    return [
        _shard_root(base) / "pending" / f"shard-{index:03d}.sqlite3"
        for index in range(shard_count)
    ]


def _remove_sqlite_database(path: Path) -> None:
    for candidate in (path, Path(f"{path}-wal"), Path(f"{path}-shm")):
        try:
            candidate.unlink()
        except FileNotFoundError:
            pass


def rectify_post_shards(database: str | Path, shard_count: int) -> list[Path]:
    """Atomically migrate the current post backlog to the configured shard count."""
    if shard_count < 1:
        raise ValueError("post shard count must be positive")
    base = Path(database)
    base.parent.mkdir(parents=True, exist_ok=True)
    root = _shard_root(base)
    root.mkdir(parents=True, exist_ok=True)
    manifest = _load_shard_manifest(base)
    active_paths = (
        [] if manifest is None else list(manifest["resolved_paths"])  # type: ignore[arg-type]
    )

    for candidate in root.iterdir():
        if candidate.is_dir() and candidate.name.startswith(".building-"):
            shutil.rmtree(candidate)

    if (
        manifest is not None
        and int(manifest.get("shard_count", 0)) == shard_count
        and not base.exists()
    ):
        for path in active_paths:
            PostQueue(path)
        active_parent = active_paths[0].parent
        for candidate in root.iterdir():
            if candidate.is_dir() and candidate != active_parent:
                shutil.rmtree(candidate)
        return active_paths

    sources = active_paths if active_paths else ([base] if base.is_file() else [])
    generation_id = uuid4().hex
    building = root / f".building-{generation_id}"
    final_generation = root / f"generation-{generation_id}"
    building.mkdir()
    building_paths = [
        building / f"shard-{index:03d}.sqlite3" for index in range(shard_count)
    ]

    for path in building_paths:
        PostQueue(path)

    target_connections: list[sqlite3.Connection] = []
    totals = {"ingested": 0, "acknowledged": 0, "retried": 0, "dead_lettered": 0}
    bucket_totals: dict[int, dict[str, float]] = {}
    cursors: dict[str, tuple[int, float]] = {}
    health: dict[str, dict[str, float | int | None]] = {}
    active_jobs = [0] * shard_count
    dead_jobs = [0] * shard_count
    metric_names = (
        "ingested",
        "detected_positive",
        "detected_negative",
        "retried",
        "dead_lettered",
        "claimed",
        "claim_wait_seconds",
        "claim_transactions",
        "claim_db_seconds",
        "completed",
        "traversal_seconds",
    )
    try:
        for path in building_paths:
            connection = sqlite3.connect(path, isolation_level=None)
            connection.row_factory = sqlite3.Row
            connection.execute("BEGIN IMMEDIATE")
            target_connections.append(connection)

        for source_path in sources:
            with sqlite3.connect(source_path) as source:
                source.row_factory = sqlite3.Row
                # Refuse to copy a database that an older coordinator is still
                # writing. The old generation remains untouched on lock failure.
                source.execute("BEGIN IMMEDIATE")
                for row in source.execute("SELECT * FROM post_jobs"):
                    shard_index = int(str(row["job_id"])[:16], 16) % shard_count
                    status = str(row["status"])
                    if status == "leased":
                        status = "pending"
                    target_connections[shard_index].execute(
                        """
                        INSERT OR IGNORE INTO post_jobs(
                            job_id, stream_id, sequence, operation_index,
                            post_uri, post_cid, author_did, text, created_at,
                            received_at, enqueued_at, status, available_at,
                            lease_until, leased_by, attempt_count, last_error
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            row["job_id"], row["stream_id"], row["sequence"],
                            row["operation_index"], row["post_uri"], row["post_cid"],
                            row["author_did"], row["text"], row["created_at"],
                            row["received_at"], row["enqueued_at"], status,
                            min(float(row["available_at"]), time.time())
                            if row["status"] == "leased" else row["available_at"],
                            None if row["status"] == "leased" else row["lease_until"],
                            None if row["status"] == "leased" else row["leased_by"],
                            row["attempt_count"], row["last_error"],
                        ),
                    )
                    if status == "dead":
                        dead_jobs[shard_index] += 1
                    else:
                        active_jobs[shard_index] += 1

                row = source.execute(
                    "SELECT * FROM post_queue_totals WHERE singleton = 1"
                ).fetchone()
                if row is not None:
                    for name in totals:
                        totals[name] += int(row[name])

                available_metric_columns = {
                    str(row["name"])
                    for row in source.execute("PRAGMA table_info(post_metric_buckets)")
                }
                select_metrics = ", ".join(
                    name if name in available_metric_columns else f"0 AS {name}"
                    for name in metric_names
                )
                for row in source.execute(
                    f"SELECT bucket, {select_metrics} FROM post_metric_buckets"
                ):
                    bucket = int(row["bucket"])
                    aggregate = bucket_totals.setdefault(
                        bucket, {name: 0.0 for name in metric_names}
                    )
                    for name in metric_names:
                        aggregate[name] += float(row[name])

                for row in source.execute("SELECT * FROM stream_cursors"):
                    stream_id = str(row["stream_id"])
                    current = cursors.get(stream_id)
                    candidate = (int(row["sequence"]), float(row["updated_at"]))
                    if current is None or candidate[0] > current[0]:
                        cursors[stream_id] = candidate

                for row in source.execute("SELECT * FROM stream_health"):
                    row_columns = set(row.keys())
                    stream_id = str(row["stream_id"])
                    aggregate = health.setdefault(
                        stream_id,
                        {
                            "connected": 0,
                            "reconnect_count": 0,
                            "received_total": 0,
                            "invalid_total": 0,
                            "last_event_at": None,
                            "source_event_at": None,
                            "updated_at": 0.0,
                        },
                    )
                    aggregate["connected"] = max(
                        int(aggregate["connected"] or 0), int(row["connected"])
                    )
                    for name in ("reconnect_count", "received_total", "invalid_total"):
                        aggregate[name] = int(aggregate[name] or 0) + int(row[name])
                    for name in ("last_event_at", "source_event_at", "updated_at"):
                        value = row[name] if name in row_columns else None
                        if value is not None:
                            aggregate[name] = max(
                                float(aggregate[name] or 0.0), float(value)
                            )

        acknowledged = totals["acknowledged"]
        historical_dead = totals["dead_lettered"]
        allocated_dead = sum(dead_jobs)
        if historical_dead > allocated_dead:
            dead_jobs[0] += historical_dead - allocated_dead
        ingested_by_shard = [
            active_jobs[index] + dead_jobs[index] for index in range(shard_count)
        ]
        ingested_by_shard[0] += acknowledged
        residual_ingested = totals["ingested"] - sum(ingested_by_shard)
        if residual_ingested > 0:
            ingested_by_shard[0] += residual_ingested

        for index, connection in enumerate(target_connections):
            connection.execute(
                """
                UPDATE post_queue_totals SET
                    ingested = ?, acknowledged = ?, retried = ?, dead_lettered = ?
                WHERE singleton = 1
                """,
                (
                    ingested_by_shard[index],
                    acknowledged if index == 0 else 0,
                    totals["retried"] if index == 0 else 0,
                    dead_jobs[index],
                ),
            )

        control = target_connections[0]
        for bucket, values in bucket_totals.items():
            control.execute(
                """
                INSERT INTO post_metric_buckets(
                    bucket, ingested, detected_positive, detected_negative,
                    retried, dead_lettered, claimed, claim_wait_seconds,
                    claim_transactions, claim_db_seconds, completed,
                    traversal_seconds
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (bucket, *(values[name] for name in metric_names)),
            )
        for stream_id, (sequence, updated_at) in cursors.items():
            control.execute(
                "INSERT INTO stream_cursors(stream_id, sequence, updated_at) VALUES (?, ?, ?)",
                (stream_id, sequence, updated_at),
            )
        for stream_id, values in health.items():
            control.execute(
                """
                INSERT INTO stream_health(
                    stream_id, connected, reconnect_count, received_total,
                    invalid_total, last_event_at, source_event_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    stream_id, values["connected"], values["reconnect_count"],
                    values["received_total"], values["invalid_total"],
                    values["last_event_at"], values["source_event_at"],
                    values["updated_at"],
                ),
            )
        for connection in target_connections:
            connection.commit()
            connection.close()
        target_connections.clear()
    except BaseException:
        for connection in target_connections:
            connection.rollback()
            connection.close()
        shutil.rmtree(building, ignore_errors=True)
        raise

    building.rename(final_generation)
    final_paths = [final_generation / path.name for path in building_paths]
    manifest_payload = {
        "version": 1,
        "shard_count": shard_count,
        "paths": [str(path.relative_to(base.parent)) for path in final_paths],
    }
    manifest_path = _shard_manifest_path(base)
    temporary_manifest = manifest_path.with_name(
        f".{manifest_path.name}.{generation_id}.tmp"
    )
    temporary_manifest.write_text(
        json.dumps(manifest_payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary_manifest.replace(manifest_path)

    if base.is_file():
        _remove_sqlite_database(base)
    old_parents = {path.parent for path in active_paths}
    for old_parent in old_parents:
        if old_parent != final_generation and old_parent.parent == root:
            shutil.rmtree(old_parent, ignore_errors=True)
    for candidate in root.iterdir():
        if candidate.is_dir() and candidate != final_generation:
            shutil.rmtree(candidate, ignore_errors=True)
    return final_paths


class ShardedPostQueue:
    """Post-queue facade that routes writes and aggregates metrics across shards."""

    def __init__(self, databases: Sequence[str | Path], *, migrate: bool = True):
        if not databases:
            raise ValueError("at least one post database shard is required")
        self.shards = [PostQueue(path, migrate=migrate) for path in databases]

    def _shard_for_job(self, job: PostJob) -> PostQueue:
        return self.shards[int(job.job_id[:16], 16) % len(self.shards)]

    def enqueue_and_checkpoint(
        self,
        jobs: Sequence[PostJob],
        *,
        stream_id: str,
        sequence: int,
        source_event_at: float | None = None,
        now: float | None = None,
    ) -> int:
        shard = self.shards[0] if not jobs else self._shard_for_job(jobs[0])
        if any(self._shard_for_job(job) is not shard for job in jobs):
            raise ValueError("one stream event cannot span multiple post shards")
        return shard.enqueue_and_checkpoint(
            jobs,
            stream_id=stream_id,
            sequence=sequence,
            source_event_at=source_event_at,
            now=now,
        )

    def get_cursor(self, stream_id: str) -> int | None:
        cursors = [shard.get_cursor(stream_id) for shard in self.shards]
        present = [cursor for cursor in cursors if cursor is not None]
        return None if not present else max(present)

    def set_stream_connected(
        self,
        stream_id: str,
        connected: bool,
        *,
        reconnect: bool = False,
        now: float | None = None,
    ) -> None:
        for index, shard in enumerate(self.shards):
            shard.set_stream_connected(
                stream_id,
                connected,
                reconnect=reconnect and index == 0,
                now=now,
            )

    def record_invalid_event(self, stream_id: str, *, now: float | None = None) -> None:
        self.shards[0].record_invalid_event(stream_id, now=now)

    def metrics(
        self,
        *,
        window_seconds: int = 60,
        now: float | None = None,
    ) -> dict[str, float | int | bool | str | None]:
        snapshots = [
            shard.metrics(window_seconds=window_seconds, now=now)
            for shard in self.shards
        ]
        sums = (
            "available", "leased", "busy_workers", "dead",
            "ingestion_rate_per_second", "label_rate_per_second",
            "detection_rate_per_second", "retry_rate_per_second",
            "recent_claimed", "recent_claim_wait_seconds",
            "recent_claim_transactions", "recent_claim_db_seconds",
            "recent_completed", "recent_traversal_seconds",
            "ingested_total", "acknowledged_total", "retried_total",
            "dead_lettered_total", "stream_reconnect_total",
            "stream_received_total", "stream_invalid_total",
        )
        result: dict[str, float | int | bool | str | None] = {
            name: sum(float(snapshot[name] or 0) for snapshot in snapshots)
            for name in sums
        }
        for name in (
            "available", "leased", "busy_workers", "dead", "recent_claimed",
            "recent_claim_transactions", "recent_completed", "ingested_total", "acknowledged_total",
            "retried_total", "dead_lettered_total", "stream_reconnect_total",
            "stream_received_total", "stream_invalid_total",
        ):
            result[name] = int(result[name] or 0)
        claimed = int(result["recent_claimed"] or 0)
        transactions = int(result["recent_claim_transactions"] or 0)
        result["average_queue_wait_seconds"] = (
            None
            if claimed == 0
            else float(result["recent_claim_wait_seconds"] or 0) / claimed
        )
        result["average_claim_seconds"] = (
            None
            if transactions == 0
            else float(result["recent_claim_db_seconds"] or 0) / transactions
        )
        completed = int(result["recent_completed"] or 0)
        result["average_traversal_seconds"] = (
            None
            if completed == 0
            else float(result["recent_traversal_seconds"] or 0) / completed
        )
        ages = [
            float(snapshot["oldest_job_age_seconds"])
            for snapshot in snapshots
            if snapshot["oldest_job_age_seconds"] is not None
        ]
        result["oldest_job_age_seconds"] = None if not ages else max(ages)
        result["stream_connected"] = any(
            bool(snapshot["stream_connected"]) for snapshot in snapshots
        )
        result["stream_id"] = next(
            (snapshot["stream_id"] for snapshot in snapshots if snapshot["stream_id"]),
            None,
        )
        for name in ("stream_last_event_age_seconds", "stream_source_lag_seconds"):
            values = [
                float(snapshot[name])
                for snapshot in snapshots
                if snapshot[name] is not None
            ]
            result[name] = None if not values else min(values)
        return result
