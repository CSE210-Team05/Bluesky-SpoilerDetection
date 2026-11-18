"""Executable throughput requirement for the performance-critical pipeline."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from benchmarks.pipeline_throughput import (
    load_requirements,
    run_pipeline_benchmark,
    write_result,
)


pytestmark = pytest.mark.performance


def test_pipeline_meets_minimum_sustained_throughput(tmp_path: Path) -> None:
    requirements = load_requirements("config/performance.json")
    artifact_directory_value = os.environ.get("PERFORMANCE_RESULTS_DIR")
    artifact_directory = (
        Path(artifact_directory_value) if artifact_directory_value else None
    )
    result = run_pipeline_benchmark(
        tmp_path,
        event_count=int(requirements["pipeline_events"]),
        minimum_throughput=float(
            requirements["minimum_pipeline_throughput_per_second"]
        ),
        benchmark_version=int(requirements["benchmark_version"]),
        profile_path=(
            artifact_directory / "pipeline.prof" if artifact_directory else None
        ),
    )
    if artifact_directory is not None:
        write_result(result, artifact_directory / "pipeline.json")
        assert (artifact_directory / "pipeline.prof").is_file()
        assert (artifact_directory / "pipeline.txt").is_file()

    assert result.passed, (
        f"pipeline processed {result.throughput_per_second:.1f} events/s; "
        f"requirement is {result.minimum_throughput_per_second:.1f} events/s"
    )
