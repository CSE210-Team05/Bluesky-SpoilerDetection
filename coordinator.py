"""Process supervisor for the complete spoiler detection pipeline."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
from typing import Any, Sequence
from urllib.request import urlopen

from Bluesky_interface.post_fetcher_interface import PostQueue
from spoiler_interface.spoiler_interface import LabelQueue


PROJECT_ROOT = Path(__file__).resolve().parent


@dataclass(frozen=True, slots=True)
class ComponentSpec:
    name: str
    command: tuple[str, ...]
    restart: bool = True
    is_reader: bool = False
    critical: bool = False
    readiness_url: str | None = None
    startup_timeout_seconds: float = 0.0


@dataclass(frozen=True, slots=True)
class SupervisionSettings:
    poll_seconds: float = 0.5
    restart_delay_seconds: float = 1.0
    max_restarts: int = 5
    shutdown_timeout_seconds: float = 5.0


def load_config(path: str | Path) -> dict[str, Any]:
    with Path(path).open(encoding="utf-8") as source:
        config = json.load(source)
    if not isinstance(config, dict):
        raise ValueError("pipeline config must be a JSON object")

    for section in ("databases", "reader", "detectors", "publishers", "metrics"):
        if not isinstance(config.get(section), dict):
            raise ValueError(f"pipeline config is missing the {section!r} object")

    for pool in ("detectors", "publishers"):
        workers = config[pool].get("workers")
        if not isinstance(workers, int) or workers < 0:
            raise ValueError(f"{pool}.workers must be a non-negative integer")
    return config


def _path_argument(value: str | Path) -> str:
    path = Path(value)
    return str(path if path.is_absolute() else PROJECT_ROOT / path)


def build_component_specs(
    config: dict[str, Any],
    *,
    python_executable: str = sys.executable,
    input_override: str | Path | None = None,
    max_events_override: int | None = None,
    force_metrics_lease: bool = False,
) -> list[ComponentSpec]:
    databases = config["databases"]
    post_database = _path_argument(databases["posts"])
    label_database = _path_argument(databases["labels"])
    specs: list[ComponentSpec] = []

    metrics = config["metrics"]
    if metrics.get("enabled", True):
        command = [
            python_executable,
            "-m",
            "metrics.gateway",
            "--post-database",
            post_database,
            "--label-database",
            label_database,
            "--config",
            _path_argument(metrics.get("tag_config", "config/spoiler_tags.json")),
            "--repository",
            str(metrics["repository"]),
            "--registry-path",
            str(metrics.get("registry_path", "docs/metrics-endpoint.json")),
            "--registry-branch",
            str(metrics.get("registry_branch", "live-metrics-endpoint-registry")),
            "--registry-source-branch",
            str(metrics.get("registry_source_branch", "main")),
            "--host",
            str(metrics.get("host", "127.0.0.1")),
            "--port",
            str(metrics.get("port", 8765)),
            "--allowed-origin",
            str(metrics["allowed_origin"]),
            "--cloudflared",
            str(metrics.get("cloudflared_executable", "cloudflared")),
            "--tunnel-timeout",
            str(metrics.get("tunnel_timeout_seconds", 30)),
            "--window-seconds",
            str(metrics.get("window_seconds", 300)),
            "--live-rate-window-seconds",
            str(metrics.get("live_rate_window_seconds", 10)),
            "--caught-up-lag-seconds",
            str(metrics.get("caught_up_lag_seconds", 10)),
        ]
        if force_metrics_lease:
            command.append("--force-lease")
        specs.append(
            ComponentSpec(
                name="metrics",
                command=tuple(command),
                restart=False,
                critical=True,
                readiness_url=(
                    f"http://{metrics.get('host', '127.0.0.1')}:"
                    f"{metrics.get('port', 8765)}/health"
                ),
                startup_timeout_seconds=float(metrics.get("startup_timeout_seconds", 60)),
            )
        )

    detectors = config["detectors"]
    for index in range(int(detectors["workers"])):
        specs.append(
            ComponentSpec(
                name=f"detector-{index + 1}",
                command=(
                    python_executable,
                    "-m",
                    "spoiler_interface.spoiler_core.spoiler_detector",
                    "--post-database",
                    post_database,
                    "--label-database",
                    label_database,
                    "--config",
                    _path_argument(detectors.get("tag_config", "config/spoiler_tags.json")),
                    "--worker-id",
                    f"detector-{index + 1}",
                    "--batch-size",
                    str(detectors.get("batch_size", 10)),
                    "--write-buffer-size",
                    str(detectors.get("write_buffer_size", 100)),
                    "--poll-seconds",
                    str(detectors.get("poll_seconds", 0.5)),
                ),
            )
        )

    publishers = config["publishers"]
    for index in range(int(publishers["workers"])):
        command = [
            python_executable,
            "-m",
            "label_publisher.publisher",
            "--database",
            label_database,
            "--worker-id",
            f"publisher-{index + 1}",
            "--batch-size",
            str(publishers.get("batch_size", 10)),
            "--poll-seconds",
            str(publishers.get("poll_seconds", 0.5)),
        ]
        if publishers.get("dry_run", False):
            command.append("--dry-run")
        specs.append(ComponentSpec(name=f"publisher-{index + 1}", command=tuple(command)))

    reader = config["reader"]
    if reader.get("enabled", True):
        command = [
            python_executable,
            "-m",
            "Bluesky_interface.post_fetcher_core.post_fetcher",
            "--database",
            post_database,
            "--stream-id",
            str(reader.get("stream_id", "jetstream-jsonl")),
        ]
        configured_input = input_override if input_override is not None else reader.get("input")
        if configured_input:
            command.extend(("--input", _path_argument(configured_input)))
        elif reader.get("endpoint"):
            command.extend(
                (
                    "--endpoint",
                    str(reader["endpoint"]),
                    "--collection",
                    str(reader.get("collection", "app.bsky.feed.post")),
                    "--open-timeout",
                    str(reader.get("open_timeout", 10)),
                    "--ping-interval",
                    str(reader.get("ping_interval", 20)),
                    "--ping-timeout",
                    str(reader.get("ping_timeout", 20)),
                    "--max-message-bytes",
                    str(reader.get("max_message_bytes", 4 * 1024 * 1024)),
                )
            )
            max_events = (
                max_events_override
                if max_events_override is not None
                else reader.get("max_events")
            )
            if max_events is not None:
                command.extend(("--max-events", str(max_events)))
        specs.append(
            ComponentSpec(
                name="reader",
                command=tuple(command),
                restart=False,
                is_reader=True,
            )
        )
    return specs


def load_supervision_settings(config: dict[str, Any]) -> SupervisionSettings:
    raw = config.get("supervision", {})
    return SupervisionSettings(
        poll_seconds=float(raw.get("poll_seconds", 0.5)),
        restart_delay_seconds=float(raw.get("restart_delay_seconds", 1.0)),
        max_restarts=int(raw.get("max_restarts", 5)),
        shutdown_timeout_seconds=float(raw.get("shutdown_timeout_seconds", 5.0)),
    )


class Coordinator:
    def __init__(
        self,
        specs: Sequence[ComponentSpec],
        *,
        post_queue: PostQueue,
        label_queue: LabelQueue,
        settings: SupervisionSettings,
    ):
        self.specs = {spec.name: spec for spec in specs}
        self.post_queue = post_queue
        self.label_queue = label_queue
        self.settings = settings
        self.processes: dict[str, subprocess.Popen[bytes]] = {}
        self.restart_counts: dict[str, int] = {name: 0 for name in self.specs}
        self.stop_requested = False
        self.reader_finished = False

    def request_stop(self, signum: int, _frame: object) -> None:
        print(f"[coordinator] received signal {signum}; shutting down", flush=True)
        self.stop_requested = True

    def _start(self, spec: ComponentSpec) -> None:
        creationflags = (
            subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
        )
        process = subprocess.Popen(
            spec.command,
            cwd=PROJECT_ROOT,
            creationflags=creationflags,
        )
        self.processes[spec.name] = process
        print(f"[coordinator] started {spec.name} pid={process.pid}", flush=True)

    def _wait_until_ready(self, spec: ComponentSpec) -> bool:
        if spec.readiness_url is None:
            return True
        process = self.processes[spec.name]
        deadline = time.monotonic() + spec.startup_timeout_seconds
        while time.monotonic() < deadline and not self.stop_requested:
            return_code = process.poll()
            if return_code is not None:
                print(
                    f"[coordinator] {spec.name} failed during startup with status {return_code}",
                    file=sys.stderr,
                    flush=True,
                )
                return False
            try:
                with urlopen(spec.readiness_url, timeout=0.5) as response:
                    payload = json.load(response)
                if payload.get("status") == "active":
                    print(f"[coordinator] {spec.name} is ready", flush=True)
                    return True
            except (OSError, ValueError):
                pass
            time.sleep(0.1)
        print(
            f"[coordinator] {spec.name} did not become ready within "
            f"{spec.startup_timeout_seconds:g}s",
            file=sys.stderr,
            flush=True,
        )
        return False

    def _queues_are_drained(self) -> bool:
        post_metrics = self.post_queue.metrics()
        label_metrics = self.label_queue.metrics()
        post_depth = int(post_metrics["available"]) + int(post_metrics["leased"])
        label_depth = int(label_metrics["available"]) + int(label_metrics["leased"])
        return post_depth == 0 and label_depth == 0

    def run(self) -> int:
        previous_handlers: dict[int, Any] = {}
        for signum in (signal.SIGINT, signal.SIGTERM):
            previous_handlers[signum] = signal.getsignal(signum)
            signal.signal(signum, self.request_stop)

        try:
            for spec in self.specs.values():
                self._start(spec)
                if not self._wait_until_ready(spec):
                    return self.processes[spec.name].poll() or 1

            while not self.stop_requested:
                for name, process in list(self.processes.items()):
                    return_code = process.poll()
                    if return_code is None:
                        continue
                    del self.processes[name]
                    spec = self.specs[name]
                    print(
                        f"[coordinator] {name} exited with status {return_code}",
                        flush=True,
                    )

                    if spec.critical:
                        print(
                            f"[coordinator] critical component {name} stopped; refusing to run",
                            file=sys.stderr,
                            flush=True,
                        )
                        return return_code or 1

                    if spec.is_reader:
                        if return_code != 0:
                            return return_code
                        self.reader_finished = True
                        continue

                    if spec.restart and not self.stop_requested:
                        self.restart_counts[name] += 1
                        if self.restart_counts[name] > self.settings.max_restarts:
                            print(
                                f"[coordinator] {name} exceeded restart limit",
                                file=sys.stderr,
                                flush=True,
                            )
                            return return_code or 1
                        time.sleep(self.settings.restart_delay_seconds)
                        self._start(spec)

                if self.reader_finished and self._queues_are_drained():
                    print("[coordinator] reader finished and queues are drained", flush=True)
                    return 0
                if not self.processes and not self.reader_finished:
                    return 0
                time.sleep(self.settings.poll_seconds)
            return 0
        finally:
            self.shutdown()
            for signum, handler in previous_handlers.items():
                signal.signal(signum, handler)

    def shutdown(self) -> None:
        running = [process for process in self.processes.values() if process.poll() is None]
        for process in running:
            if os.name == "nt":
                process.send_signal(signal.CTRL_BREAK_EVENT)
            else:
                process.terminate()

        deadline = time.monotonic() + self.settings.shutdown_timeout_seconds
        for process in running:
            remaining = max(0.0, deadline - time.monotonic())
            try:
                process.wait(timeout=remaining)
            except subprocess.TimeoutExpired:
                process.kill()
        for process in running:
            if process.poll() is None:
                process.wait()
        self.processes.clear()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Start and supervise the spoiler pipeline")
    parser.add_argument("--config", type=Path, default=Path("config/pipeline.json"))
    parser.add_argument("--input", type=Path, help="override reader JSON Lines input")
    parser.add_argument(
        "--max-events",
        type=int,
        help="live smoke test: stop after this many Jetstream messages",
    )
    parser.add_argument(
        "--force-metrics-lease",
        action="store_true",
        help="replace a stale remote metrics lease after verifying its owner has stopped",
    )
    args = parser.parse_args(argv)

    config = load_config(args.config)
    specs = build_component_specs(
        config,
        input_override=args.input,
        max_events_override=args.max_events,
        force_metrics_lease=args.force_metrics_lease,
    )
    databases = config["databases"]
    coordinator = Coordinator(
        specs,
        post_queue=PostQueue(_path_argument(databases["posts"])),
        label_queue=LabelQueue(_path_argument(databases["labels"])),
        settings=load_supervision_settings(config),
    )
    return coordinator.run()


if __name__ == "__main__":
    raise SystemExit(main())
