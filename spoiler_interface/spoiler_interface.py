"""Public interface for detected spoilers and the label work queue."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
import sqlite3
import time
from typing import Iterator


@dataclass(frozen=True, slots=True)
class LabelJob:
    job_id: str
    post_uri: str
    post_cid: str
    label_value: str
    detected_at: float
    detector_version: str

    @classmethod
    def create(
        cls,
        *,
        post_uri: str,
        post_cid: str,
        label_value: str,
        detector_version: str,
        detected_at: float | None = None,
    ) -> "LabelJob":
        identity = f"{post_uri}\0{post_cid}\0{label_value}".encode()
        return cls(
            job_id=sha256(identity).hexdigest(),
            post_uri=post_uri,
            post_cid=post_cid,
            label_value=label_value,
            detected_at=time.time() if detected_at is None else detected_at,
            detector_version=detector_version,
        )


@dataclass(frozen=True, slots=True)
class ClaimedLabel:
    job: LabelJob
    attempt_count: int
    lease_until: float


class LabelQueue:
    """SQLite-backed competing-consumer queue for CID-scoped labels."""

    _METRIC_COLUMNS = {"enqueued", "published", "retried", "dead_lettered"}

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
                CREATE TABLE IF NOT EXISTS label_jobs (
                    job_id TEXT PRIMARY KEY,
                    post_uri TEXT NOT NULL,
                    post_cid TEXT NOT NULL,
                    label_value TEXT NOT NULL,
                    detected_at REAL NOT NULL,
                    detector_version TEXT NOT NULL,
                    enqueued_at REAL NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending', 'leased', 'dead')),
                    available_at REAL NOT NULL,
                    lease_until REAL,
                    leased_by TEXT,
                    attempt_count INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT,
                    UNIQUE(post_uri, post_cid, label_value)
                );

                CREATE INDEX IF NOT EXISTS label_jobs_claimable
                    ON label_jobs(status, available_at, lease_until, detected_at);

                CREATE INDEX IF NOT EXISTS label_jobs_active_age
                    ON label_jobs(enqueued_at) WHERE status != 'dead';

                CREATE TABLE IF NOT EXISTS published_labels (
                    job_id TEXT PRIMARY KEY,
                    post_uri TEXT NOT NULL,
                    post_cid TEXT NOT NULL,
                    label_value TEXT NOT NULL,
                    published_at REAL NOT NULL,
                    UNIQUE(post_uri, post_cid, label_value)
                );

                CREATE TABLE IF NOT EXISTS label_queue_totals (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    enqueued INTEGER NOT NULL DEFAULT 0,
                    published INTEGER NOT NULL DEFAULT 0,
                    retried INTEGER NOT NULL DEFAULT 0,
                    dead_lettered INTEGER NOT NULL DEFAULT 0
                );

                INSERT OR IGNORE INTO label_queue_totals(singleton) VALUES (1);

                CREATE TABLE IF NOT EXISTS label_metric_buckets (
                    bucket INTEGER PRIMARY KEY,
                    enqueued INTEGER NOT NULL DEFAULT 0,
                    published INTEGER NOT NULL DEFAULT 0,
                    retried INTEGER NOT NULL DEFAULT 0,
                    dead_lettered INTEGER NOT NULL DEFAULT 0
                );
                """
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
            INSERT INTO label_metric_buckets(bucket, {metric}) VALUES (?, ?)
            ON CONFLICT(bucket) DO UPDATE
                SET {metric} = {metric} + excluded.{metric}
            """,
            (bucket, amount),
        )

    def enqueue(self, job: LabelJob, *, now: float | None = None) -> bool:
        timestamp = time.time() if now is None else now
        with self._connect() as connection:
            self._begin(connection)
            try:
                result = connection.execute(
                    """
                    INSERT OR IGNORE INTO label_jobs(
                        job_id, post_uri, post_cid, label_value, detected_at,
                        detector_version, enqueued_at, available_at
                    ) SELECT ?, ?, ?, ?, ?, ?, ?, ?
                    WHERE NOT EXISTS (
                        SELECT 1 FROM published_labels WHERE job_id = ?
                    )
                    """,
                    (
                        job.job_id,
                        job.post_uri,
                        job.post_cid,
                        job.label_value,
                        job.detected_at,
                        job.detector_version,
                        timestamp,
                        timestamp,
                        job.job_id,
                    ),
                )
                inserted = result.rowcount == 1
                if inserted:
                    connection.execute(
                        "UPDATE label_queue_totals SET enqueued = enqueued + 1 WHERE singleton = 1"
                    )
                    self._increment_metric(connection, "enqueued", 1, timestamp)
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
        return inserted

    def claim(
        self,
        worker_id: str,
        *,
        limit: int = 1,
        lease_seconds: float = 60.0,
        now: float | None = None,
    ) -> list[ClaimedLabel]:
        if limit < 1:
            raise ValueError("limit must be positive")
        timestamp = time.time() if now is None else now
        lease_until = timestamp + lease_seconds
        with self._connect() as connection:
            self._begin(connection)
            try:
                rows = connection.execute(
                    """
                    SELECT * FROM label_jobs
                    WHERE status != 'dead'
                      AND available_at <= ?
                      AND (status = 'pending' OR lease_until <= ?)
                    ORDER BY detected_at, job_id
                    LIMIT ?
                    """,
                    (timestamp, timestamp, limit),
                ).fetchall()
                job_ids = [row["job_id"] for row in rows]
                if job_ids:
                    placeholders = ",".join("?" for _ in job_ids)
                    connection.execute(
                        f"""
                        UPDATE label_jobs
                        SET status = 'leased', leased_by = ?, lease_until = ?,
                            attempt_count = attempt_count + 1
                        WHERE job_id IN ({placeholders})
                        """,
                        (worker_id, lease_until, *job_ids),
                    )
                    rows = connection.execute(
                        f"SELECT * FROM label_jobs WHERE job_id IN ({placeholders}) "
                        "ORDER BY detected_at, job_id",
                        job_ids,
                    ).fetchall()
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

        return [
            ClaimedLabel(
                job=self._row_to_job(row),
                attempt_count=int(row["attempt_count"]),
                lease_until=float(row["lease_until"]),
            )
            for row in rows
        ]

    def acknowledge(self, job_id: str, worker_id: str, *, now: float | None = None) -> bool:
        timestamp = time.time() if now is None else now
        with self._connect() as connection:
            self._begin(connection)
            try:
                row = connection.execute(
                    """
                    SELECT job_id, post_uri, post_cid, label_value
                    FROM label_jobs
                    WHERE job_id = ? AND status = 'leased' AND leased_by = ?
                    """,
                    (job_id, worker_id),
                ).fetchone()
                acknowledged = row is not None
                if acknowledged:
                    connection.execute(
                        """
                        INSERT OR IGNORE INTO published_labels(
                            job_id, post_uri, post_cid, label_value, published_at
                        ) VALUES (?, ?, ?, ?, ?)
                        """,
                        (
                            row["job_id"],
                            row["post_uri"],
                            row["post_cid"],
                            row["label_value"],
                            timestamp,
                        ),
                    )
                    connection.execute("DELETE FROM label_jobs WHERE job_id = ?", (job_id,))
                    connection.execute(
                        "UPDATE label_queue_totals SET published = published + 1 WHERE singleton = 1"
                    )
                    self._increment_metric(connection, "published", 1, timestamp)
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
                    SELECT attempt_count FROM label_jobs
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
                    UPDATE label_jobs SET status = ?, available_at = ?,
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
                    f"UPDATE label_queue_totals SET {total_column} = {total_column} + 1 "
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

    def metrics(self, *, window_seconds: int = 60, now: float | None = None) -> dict[str, float | int | None]:
        timestamp = time.time() if now is None else now
        first_bucket = int((timestamp - window_seconds) // 60) * 60
        with self._connect() as connection:
            connection.execute("BEGIN")
            totals = connection.execute(
                "SELECT * FROM label_queue_totals WHERE singleton = 1"
            ).fetchone()
            leased_row = connection.execute(
                """
                SELECT COUNT(*) AS leased
                FROM label_jobs
                WHERE status = 'leased' AND lease_until > ?
                """,
                (timestamp,),
            ).fetchone()
            oldest_row = connection.execute(
                """
                SELECT MIN(enqueued_at) AS oldest
                FROM label_jobs WHERE status != 'dead'
                """
            ).fetchone()
            rates = connection.execute(
                """
                SELECT COALESCE(SUM(enqueued), 0) AS enqueued,
                       COALESCE(SUM(published), 0) AS published,
                       COALESCE(SUM(retried), 0) AS retried
                FROM label_metric_buckets WHERE bucket >= ?
                """,
                (first_bucket,),
            ).fetchone()
            connection.commit()

        dead = int(totals["dead_lettered"])
        depth = max(0, int(totals["enqueued"]) - int(totals["published"]) - dead)
        leased = min(depth, int(leased_row["leased"] or 0))
        oldest = oldest_row["oldest"]
        return {
            "available": depth - leased,
            "leased": leased,
            "dead": dead,
            "oldest_job_age_seconds": None if oldest is None else max(0.0, timestamp - float(oldest)),
            "enqueued_rate_per_second": float(rates["enqueued"]) / window_seconds,
            "publish_rate_per_second": float(rates["published"]) / window_seconds,
            "retry_rate_per_second": float(rates["retried"]) / window_seconds,
            "enqueued_total": int(totals["enqueued"]),
            "published_total": int(totals["published"]),
            "retried_total": int(totals["retried"]),
            "dead_lettered_total": int(totals["dead_lettered"]),
        }

    @staticmethod
    def _row_to_job(row: sqlite3.Row) -> LabelJob:
        return LabelJob(
            job_id=row["job_id"],
            post_uri=row["post_uri"],
            post_cid=row["post_cid"],
            label_value=row["label_value"],
            detected_at=float(row["detected_at"]),
            detector_version=row["detector_version"],
        )
