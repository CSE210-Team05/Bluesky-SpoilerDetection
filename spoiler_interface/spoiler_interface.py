"""Public interface for detected spoilers and the label work queue."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from hashlib import sha256
import json
from pathlib import Path
import queue
import shutil
import sqlite3
import threading
import time
from typing import Iterator, Sequence
from uuid import uuid4

from Bluesky_interface.post_fetcher_interface import ClaimedPost, PostQueue


@dataclass(frozen=True, slots=True)
class LabelJob:
    job_id: str
    post_uri: str
    post_cid: str
    label_value: str
    detected_at: float
    detector_version: str
    pipeline_started_at: float

    @classmethod
    def create(
        cls,
        *,
        post_uri: str,
        post_cid: str,
        label_value: str,
        detector_version: str,
        detected_at: float | None = None,
        pipeline_started_at: float | None = None,
    ) -> "LabelJob":
        identity = f"{post_uri}\0{post_cid}\0{label_value}".encode()
        detection_timestamp = time.time() if detected_at is None else detected_at
        return cls(
            job_id=sha256(identity).hexdigest(),
            post_uri=post_uri,
            post_cid=post_cid,
            label_value=label_value,
            detected_at=detection_timestamp,
            detector_version=detector_version,
            pipeline_started_at=(
                detection_timestamp
                if pipeline_started_at is None
                else pipeline_started_at
            ),
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
                    pipeline_started_at REAL NOT NULL,
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
            job_columns = {
                str(row["name"])
                for row in connection.execute("PRAGMA table_info(label_jobs)")
            }
            if "pipeline_started_at" not in job_columns:
                connection.execute(
                    "ALTER TABLE label_jobs ADD COLUMN pipeline_started_at REAL"
                )
                connection.execute(
                    "UPDATE label_jobs SET pipeline_started_at = detected_at "
                    "WHERE pipeline_started_at IS NULL"
                )
            metric_columns = {
                str(row["name"])
                for row in connection.execute("PRAGMA table_info(label_metric_buckets)")
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
                        f"ALTER TABLE label_metric_buckets ADD COLUMN {column} {definition}"
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
                        detector_version, pipeline_started_at, enqueued_at,
                        available_at
                    ) SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?
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
                        job.pipeline_started_at,
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
        claim_started = time.perf_counter()
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
                    wait_seconds = sum(
                        max(0.0, timestamp - float(row["enqueued_at"])) for row in rows
                    )
                    claim_seconds = time.perf_counter() - claim_started
                    bucket = int(timestamp // 60) * 60
                    connection.execute(
                        """
                        INSERT INTO label_metric_buckets(
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
                    SELECT job_id, post_uri, post_cid, label_value,
                           pipeline_started_at
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
                    bucket = int(timestamp // 60) * 60
                    connection.execute(
                        """
                        INSERT INTO label_metric_buckets(
                            bucket, completed, traversal_seconds
                        ) VALUES (?, 1, ?)
                        ON CONFLICT(bucket) DO UPDATE SET
                            completed = completed + 1,
                            traversal_seconds = traversal_seconds
                                + excluded.traversal_seconds
                        """,
                        (
                            bucket,
                            max(
                                0.0,
                                timestamp - float(row["pipeline_started_at"]),
                            ),
                        ),
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
                SELECT COUNT(*) AS leased,
                       COUNT(DISTINCT leased_by) AS busy_workers
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
                       COALESCE(SUM(retried), 0) AS retried,
                       COALESCE(SUM(claimed), 0) AS claimed,
                       COALESCE(SUM(claim_wait_seconds), 0) AS claim_wait_seconds,
                       COALESCE(SUM(claim_transactions), 0) AS claim_transactions,
                       COALESCE(SUM(claim_db_seconds), 0) AS claim_db_seconds,
                       COALESCE(SUM(completed), 0) AS completed,
                       COALESCE(SUM(traversal_seconds), 0) AS traversal_seconds
                FROM label_metric_buckets WHERE bucket >= ?
                """,
                (first_bucket,),
            ).fetchone()
            connection.commit()

        dead = int(totals["dead_lettered"])
        depth = max(0, int(totals["enqueued"]) - int(totals["published"]) - dead)
        leased = min(depth, int(leased_row["leased"] or 0))
        claimed = int(rates["claimed"] or 0)
        claim_transactions = int(rates["claim_transactions"] or 0)
        completed = int(rates["completed"] or 0)
        oldest = oldest_row["oldest"]
        return {
            "available": depth - leased,
            "leased": leased,
            "busy_workers": int(leased_row["busy_workers"] or 0),
            "dead": dead,
            "oldest_job_age_seconds": None if oldest is None else max(0.0, timestamp - float(oldest)),
            "enqueued_rate_per_second": float(rates["enqueued"]) / window_seconds,
            "publish_rate_per_second": float(rates["published"]) / window_seconds,
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
            pipeline_started_at=float(row["pipeline_started_at"]),
        )


def _label_shard_manifest_path(database: Path) -> Path:
    return database.with_name(f"{database.name}.shards.json")


def _label_shard_root(database: Path) -> Path:
    return database.with_name(f"{database.name}.shards")


def _load_label_shard_manifest(database: Path) -> dict[str, object] | None:
    manifest_path = _label_shard_manifest_path(database)
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    if not isinstance(payload, dict) or payload.get("version") != 1:
        raise RuntimeError(f"invalid label-shard manifest: {manifest_path}")
    raw_paths = payload.get("paths")
    if not isinstance(raw_paths, list) or not all(
        isinstance(path, str) for path in raw_paths
    ):
        raise RuntimeError(f"invalid label-shard paths in manifest: {manifest_path}")
    paths = [database.parent / str(path) for path in raw_paths]
    if not paths or any(not path.is_file() for path in paths):
        raise RuntimeError(
            f"label-shard manifest references missing databases: {manifest_path}"
        )
    return {**payload, "resolved_paths": paths}


def configured_label_shard_paths(database: str | Path, shard_count: int) -> list[Path]:
    """Return active label shards, or placeholders before startup rectification."""
    if shard_count < 1:
        raise ValueError("label shard count must be positive")
    base = Path(database)
    manifest = _load_label_shard_manifest(base)
    if manifest is not None and int(manifest.get("shard_count", 0)) == shard_count:
        return list(manifest["resolved_paths"])  # type: ignore[arg-type]
    return [
        _label_shard_root(base) / "pending" / f"shard-{index:03d}.sqlite3"
        for index in range(shard_count)
    ]


def _remove_label_sqlite_database(path: Path) -> None:
    for candidate in (path, Path(f"{path}-wal"), Path(f"{path}-shm")):
        try:
            candidate.unlink()
        except FileNotFoundError:
            pass


def rectify_label_shards(database: str | Path, shard_count: int) -> list[Path]:
    """Atomically migrate label backlog and publication history into shards."""
    if shard_count < 1:
        raise ValueError("label shard count must be positive")
    base = Path(database)
    base.parent.mkdir(parents=True, exist_ok=True)
    root = _label_shard_root(base)
    root.mkdir(parents=True, exist_ok=True)
    manifest = _load_label_shard_manifest(base)
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
            LabelQueue(path)
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
        LabelQueue(path)

    target_connections: list[sqlite3.Connection] = []
    totals = {"enqueued": 0, "published": 0, "retried": 0, "dead_lettered": 0}
    bucket_totals: dict[int, dict[str, float]] = {}
    active_jobs = [0] * shard_count
    dead_jobs = [0] * shard_count
    published_jobs = [0] * shard_count
    metric_names = (
        "enqueued", "published", "retried", "dead_lettered", "claimed",
        "claim_wait_seconds", "claim_transactions", "claim_db_seconds",
        "completed", "traversal_seconds",
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
                source.execute("BEGIN IMMEDIATE")
                source_job_columns = {
                    str(row["name"])
                    for row in source.execute("PRAGMA table_info(label_jobs)")
                }
                for row in source.execute("SELECT * FROM label_jobs"):
                    shard_index = int(str(row["job_id"])[:16], 16) % shard_count
                    status = str(row["status"])
                    if status == "leased":
                        status = "pending"
                    target_connections[shard_index].execute(
                        """
                        INSERT OR IGNORE INTO label_jobs(
                            job_id, post_uri, post_cid, label_value, detected_at,
                            detector_version, pipeline_started_at, enqueued_at,
                            status, available_at, lease_until, leased_by,
                            attempt_count, last_error
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            row["job_id"], row["post_uri"], row["post_cid"],
                            row["label_value"], row["detected_at"],
                            row["detector_version"],
                            (
                                row["pipeline_started_at"]
                                if "pipeline_started_at" in source_job_columns
                                else row["detected_at"]
                            ),
                            row["enqueued_at"], status,
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

                for row in source.execute("SELECT * FROM published_labels"):
                    shard_index = int(str(row["job_id"])[:16], 16) % shard_count
                    target_connections[shard_index].execute(
                        """
                        INSERT OR IGNORE INTO published_labels(
                            job_id, post_uri, post_cid, label_value, published_at
                        ) VALUES (?, ?, ?, ?, ?)
                        """,
                        (
                            row["job_id"], row["post_uri"], row["post_cid"],
                            row["label_value"], row["published_at"],
                        ),
                    )
                    published_jobs[shard_index] += 1

                row = source.execute(
                    "SELECT * FROM label_queue_totals WHERE singleton = 1"
                ).fetchone()
                if row is not None:
                    for name in totals:
                        totals[name] += int(row[name])

                available_metric_columns = {
                    str(row["name"])
                    for row in source.execute("PRAGMA table_info(label_metric_buckets)")
                }
                select_metrics = ", ".join(
                    name if name in available_metric_columns else f"0 AS {name}"
                    for name in metric_names
                )
                for row in source.execute(
                    f"SELECT bucket, {select_metrics} FROM label_metric_buckets"
                ):
                    bucket = int(row["bucket"])
                    aggregate = bucket_totals.setdefault(
                        bucket, {name: 0.0 for name in metric_names}
                    )
                    for name in metric_names:
                        aggregate[name] += float(row[name])

        historical_dead = totals["dead_lettered"]
        if historical_dead > sum(dead_jobs):
            dead_jobs[0] += historical_dead - sum(dead_jobs)
        published_total = totals["published"]
        if published_total > sum(published_jobs):
            published_jobs[0] += published_total - sum(published_jobs)
        enqueued_by_shard = [
            active_jobs[index] + dead_jobs[index] + published_jobs[index]
            for index in range(shard_count)
        ]
        residual_enqueued = totals["enqueued"] - sum(enqueued_by_shard)
        if residual_enqueued > 0:
            enqueued_by_shard[0] += residual_enqueued

        for index, connection in enumerate(target_connections):
            connection.execute(
                """
                UPDATE label_queue_totals SET
                    enqueued = ?, published = ?, retried = ?, dead_lettered = ?
                WHERE singleton = 1
                """,
                (
                    enqueued_by_shard[index], published_jobs[index],
                    totals["retried"] if index == 0 else 0, dead_jobs[index],
                ),
            )

        control = target_connections[0]
        for bucket, values in bucket_totals.items():
            control.execute(
                """
                INSERT INTO label_metric_buckets(
                    bucket, enqueued, published, retried, dead_lettered,
                    claimed, claim_wait_seconds, claim_transactions,
                    claim_db_seconds, completed, traversal_seconds
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (bucket, *(values[name] for name in metric_names)),
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
    manifest_path = _label_shard_manifest_path(base)
    temporary_manifest = manifest_path.with_name(
        f".{manifest_path.name}.{generation_id}.tmp"
    )
    temporary_manifest.write_text(
        json.dumps(manifest_payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary_manifest.replace(manifest_path)

    if base.is_file():
        _remove_label_sqlite_database(base)
    old_parents = {path.parent for path in active_paths}
    for old_parent in old_parents:
        if old_parent != final_generation and old_parent.parent == root:
            shutil.rmtree(old_parent, ignore_errors=True)
    for candidate in root.iterdir():
        if candidate.is_dir() and candidate != final_generation:
            shutil.rmtree(candidate, ignore_errors=True)
    return final_paths


class ShardedLabelQueue:
    """Label-queue facade that routes writes and aggregates shard metrics."""

    def __init__(self, databases: Sequence[str | Path], *, migrate: bool = True):
        if not databases:
            raise ValueError("at least one label database shard is required")
        self.shards = [LabelQueue(path, migrate=migrate) for path in databases]

    def _shard_for_job_id(self, job_id: str) -> LabelQueue:
        return self.shards[int(job_id[:16], 16) % len(self.shards)]

    def enqueue(self, job: LabelJob, *, now: float | None = None) -> bool:
        return self._shard_for_job_id(job.job_id).enqueue(job, now=now)

    def claim(
        self,
        worker_id: str,
        *,
        limit: int = 1,
        lease_seconds: float = 60.0,
        now: float | None = None,
    ) -> list[ClaimedLabel]:
        deliveries: list[ClaimedLabel] = []
        for shard in self.shards:
            if len(deliveries) >= limit:
                break
            deliveries.extend(
                shard.claim(
                    worker_id,
                    limit=limit - len(deliveries),
                    lease_seconds=lease_seconds,
                    now=now,
                )
            )
        return deliveries

    def acknowledge(
        self,
        job_id: str,
        worker_id: str,
        *,
        now: float | None = None,
    ) -> bool:
        return self._shard_for_job_id(job_id).acknowledge(job_id, worker_id, now=now)

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
        return self._shard_for_job_id(job_id).retry(
            job_id,
            worker_id,
            error,
            delay_seconds=delay_seconds,
            max_attempts=max_attempts,
            now=now,
        )

    def metrics(
        self,
        *,
        window_seconds: int = 60,
        now: float | None = None,
    ) -> dict[str, float | int | None]:
        snapshots = [
            shard.metrics(window_seconds=window_seconds, now=now)
            for shard in self.shards
        ]
        sums = (
            "available", "leased", "busy_workers", "dead",
            "enqueued_rate_per_second", "publish_rate_per_second",
            "retry_rate_per_second", "recent_claimed",
            "recent_claim_wait_seconds", "recent_claim_transactions",
            "recent_claim_db_seconds", "recent_completed",
            "recent_traversal_seconds", "enqueued_total", "published_total",
            "retried_total", "dead_lettered_total",
        )
        result: dict[str, float | int | None] = {
            name: sum(float(snapshot[name] or 0) for snapshot in snapshots)
            for name in sums
        }
        for name in (
            "available", "leased", "busy_workers", "dead", "recent_claimed",
            "recent_claim_transactions", "recent_completed", "enqueued_total", "published_total",
            "retried_total", "dead_lettered_total",
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
        return result


@dataclass(frozen=True, slots=True)
class PostIdentifier:
    """Opaque detector result ID containing the fields needed for a label."""

    job_id: str
    post_uri: str
    post_cid: str


@dataclass(frozen=True, slots=True)
class SpoilerItem:
    """The complete input contract exposed to spoiler-classification code."""

    id: PostIdentifier
    text: str


@dataclass(frozen=True, slots=True)
class _Completion:
    delivery: ClaimedPost
    is_spoiler: bool | None
    error: str | None
    detected_at: float


_STOP_WRITER = object()


class SpoilerWorkerInterface:
    """Worker-facing post source with an asynchronous durable result sink.

    The detector sees only ``SpoilerItem`` values and submits an ID plus a
    boolean. This adapter owns leases, retries, label writes, and source-post
    acknowledgements. Positive results are always inserted into the label
    queue before their source post is acknowledged.
    """

    def __init__(
        self,
        post_queue: PostQueue,
        label_queue: LabelQueue | ShardedLabelQueue,
        worker_id: str,
        *,
        label_value: str,
        detector_version: str,
        lease_seconds: float = 60.0,
        retry_delay_seconds: float = 5.0,
        max_attempts: int = 5,
        max_pending_writes: int = 100,
    ):
        if max_pending_writes < 1:
            raise ValueError("max_pending_writes must be positive")
        self.post_queue = post_queue
        self.label_queue = label_queue
        self.worker_id = worker_id
        self.label_value = label_value
        self.detector_version = detector_version
        self.lease_seconds = lease_seconds
        self.retry_delay_seconds = retry_delay_seconds
        self.max_attempts = max_attempts
        self.max_pending_writes = max_pending_writes
        self._completions: queue.Queue[_Completion | object] = queue.Queue(
            maxsize=max_pending_writes
        )
        self._inflight: dict[PostIdentifier, ClaimedPost] = {}
        self._outstanding = 0
        self._counts = {"positive": 0, "negative": 0, "failed": 0}
        self._background_errors: list[BaseException] = []
        self._lock = threading.Lock()
        self._closed = False
        self._writer = threading.Thread(
            target=self._write_completions,
            name=f"{worker_id}-result-writer",
            daemon=True,
        )
        self._writer.start()

    def __enter__(self) -> "SpoilerWorkerInterface":
        return self

    def __exit__(self, _type: object, _value: object, _traceback: object) -> None:
        self.close()

    def _raise_background_error(self) -> None:
        with self._lock:
            error = self._background_errors[0] if self._background_errors else None
        if error is not None:
            raise RuntimeError("asynchronous result writer failed") from error

    def get_items(self, *, limit: int = 1) -> list[SpoilerItem]:
        """Atomically claim up to ``limit`` posts and expose logical inputs."""
        if limit < 1:
            raise ValueError("limit must be positive")
        if self._closed:
            raise RuntimeError("worker interface is closed")
        self._raise_background_error()
        with self._lock:
            capacity = self.max_pending_writes - self._outstanding
        if capacity <= 0:
            return []

        deliveries = self.post_queue.claim(
            self.worker_id,
            limit=min(limit, capacity),
            lease_seconds=self.lease_seconds,
        )
        items: list[SpoilerItem] = []
        with self._lock:
            for delivery in deliveries:
                identifier = PostIdentifier(
                    job_id=delivery.job.job_id,
                    post_uri=delivery.job.post_uri,
                    post_cid=delivery.job.post_cid,
                )
                self._inflight[identifier] = delivery
                self._outstanding += 1
                items.append(SpoilerItem(id=identifier, text=delivery.job.text))
        return items

    def _take_delivery(self, identifier: PostIdentifier) -> ClaimedPost:
        with self._lock:
            try:
                return self._inflight.pop(identifier)
            except KeyError as error:
                raise ValueError("result ID is not leased by this worker") from error

    def submit_result(self, identifier: PostIdentifier, is_spoiler: bool) -> None:
        """Queue a classification result without waiting for either database."""
        if self._closed:
            raise RuntimeError("worker interface is closed")
        delivery = self._take_delivery(identifier)
        self._completions.put(
            _Completion(
                delivery=delivery,
                is_spoiler=bool(is_spoiler),
                error=None,
                detected_at=time.time(),
            )
        )

    def submit_error(self, identifier: PostIdentifier, error: BaseException) -> None:
        """Return a failed classification to the queue through the same sink."""
        if self._closed:
            raise RuntimeError("worker interface is closed")
        delivery = self._take_delivery(identifier)
        self._completions.put(
            _Completion(
                delivery=delivery,
                is_spoiler=None,
                error=f"{type(error).__name__}: {error}",
                detected_at=time.time(),
            )
        )

    def _retry(self, completion: _Completion, error: str) -> None:
        self.post_queue.retry(
            completion.delivery.job.job_id,
            self.worker_id,
            error,
            delay_seconds=self.retry_delay_seconds,
            max_attempts=self.max_attempts,
        )

    def _persist(self, completion: _Completion) -> str:
        if completion.error is not None:
            self._retry(completion, completion.error)
            return "failed"

        outcome = "positive" if completion.is_spoiler else "negative"
        if completion.is_spoiler:
            self.label_queue.enqueue(
                LabelJob.create(
                    post_uri=completion.delivery.job.post_uri,
                    post_cid=completion.delivery.job.post_cid,
                    label_value=self.label_value,
                    detector_version=self.detector_version,
                    detected_at=completion.detected_at,
                    pipeline_started_at=completion.delivery.job.received_at,
                )
            )
        acknowledged = self.post_queue.acknowledge(
            completion.delivery.job.job_id,
            self.worker_id,
            outcome=outcome,
        )
        if not acknowledged:
            raise RuntimeError("post lease was lost before acknowledgement")
        return outcome

    def _write_completions(self) -> None:
        while True:
            queued = self._completions.get()
            if queued is _STOP_WRITER:
                self._completions.task_done()
                return
            assert isinstance(queued, _Completion)
            outcome = "failed"
            try:
                outcome = self._persist(queued)
            except Exception as error:
                try:
                    self._retry(queued, f"{type(error).__name__}: {error}")
                except Exception as retry_error:
                    with self._lock:
                        self._background_errors.append(retry_error)
            finally:
                with self._lock:
                    self._counts[outcome] += 1
                    self._outstanding -= 1
                self._completions.task_done()

    def flush(self) -> dict[str, int]:
        """Wait until all submitted results are durable and return sink totals."""
        self._completions.join()
        self._raise_background_error()
        with self._lock:
            return dict(self._counts)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        with self._lock:
            abandoned = list(self._inflight.values())
            self._inflight.clear()
        for delivery in abandoned:
            self._completions.put(
                _Completion(
                    delivery=delivery,
                    is_spoiler=None,
                    error="worker stopped before classification completed",
                    detected_at=time.time(),
                )
            )

        error: BaseException | None = None
        try:
            self.flush()
        except BaseException as caught:
            error = caught
        self._completions.put(_STOP_WRITER)
        self._completions.join()
        self._writer.join()
        if error is not None:
            raise error
