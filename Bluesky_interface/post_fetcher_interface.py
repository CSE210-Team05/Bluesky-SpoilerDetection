"""Public interface for the Bluesky reader and post work queue.

Only this module owns the post queue schema.  Reader and detector processes may
each create an adapter instance, but they should not issue SQL directly.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
import sqlite3
import time
from typing import Iterator, Sequence


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
                    dead_lettered INTEGER NOT NULL DEFAULT 0
                );
                """
            )
            stream_health_columns = {
                str(row["name"])
                for row in connection.execute("PRAGMA table_info(stream_health)")
            }
            if "source_event_at" not in stream_health_columns:
                connection.execute("ALTER TABLE stream_health ADD COLUMN source_event_at REAL")

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
                result = connection.execute(
                    "DELETE FROM post_jobs WHERE job_id = ? AND status = 'leased' AND leased_by = ?",
                    (job_id, worker_id),
                )
                acknowledged = result.rowcount == 1
                if acknowledged:
                    connection.execute(
                        "UPDATE post_queue_totals SET acknowledged = acknowledged + 1 WHERE singleton = 1"
                    )
                    self._increment_metric(
                        connection,
                        f"detected_{outcome}",
                        1,
                        timestamp,
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
                SELECT COUNT(*) AS leased
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
                       COALESCE(SUM(retried), 0) AS retried
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
        oldest = oldest_row["oldest"]
        result: dict[str, float | int | bool | str | None] = {
            "available": depth - leased,
            "leased": leased,
            "dead": dead,
            "oldest_job_age_seconds": None if oldest is None else max(0.0, timestamp - float(oldest)),
            "ingestion_rate_per_second": float(rates["ingested"]) / window_seconds,
            "label_rate_per_second": float(rates["positive"]) / window_seconds,
            "detection_rate_per_second": (float(rates["positive"]) + float(rates["negative"])) / window_seconds,
            "retry_rate_per_second": float(rates["retried"]) / window_seconds,
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
