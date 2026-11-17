"""Periodically publish the aggregate metrics snapshot to GitHub Pages."""

from __future__ import annotations

import argparse
import base64
import json
import os
from pathlib import Path
import sys
import time
from typing import Any, Callable
from urllib.error import HTTPError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

from Bluesky_interface.post_fetcher_interface import PostQueue
from metrics.snapshot import build_snapshot
from spoiler_interface.spoiler_interface import LabelQueue


API_VERSION = "2022-11-28"


class GitHubContentsPublisher:
    def __init__(
        self,
        *,
        repository: str,
        path: str,
        branch: str,
        token: str,
        opener: Callable[..., Any] = urlopen,
    ):
        if repository.count("/") != 1:
            raise ValueError("repository must have OWNER/REPOSITORY form")
        owner, name = repository.split("/", 1)
        self.url = (
            f"https://api.github.com/repos/{quote(owner)}/{quote(name)}/contents/"
            f"{quote(path.lstrip('/'), safe='/')}"
        )
        self.branch = branch
        self.token = token
        self.opener = opener

    def _request(self, method: str, *, body: dict[str, Any] | None = None) -> Request:
        data = None if body is None else json.dumps(body).encode("utf-8")
        return Request(
            self.url,
            data=data,
            method=method,
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json",
                "User-Agent": "bluesky-spoiler-metrics",
                "X-GitHub-Api-Version": API_VERSION,
            },
        )

    def current_sha(self) -> str | None:
        request = self._request("GET")
        request.full_url = f"{request.full_url}?{urlencode({'ref': self.branch})}"
        try:
            with self.opener(request, timeout=15) as response:
                payload = json.load(response)
        except HTTPError as error:
            if error.code == 404:
                return None
            raise
        sha = payload.get("sha")
        if not isinstance(sha, str):
            raise ValueError("GitHub contents response did not contain a file SHA")
        return sha

    def publish(self, snapshot: dict[str, Any]) -> str:
        content = (json.dumps(snapshot, indent=2, sort_keys=True) + "\n").encode("utf-8")
        body: dict[str, Any] = {
            "message": "Update pipeline metrics",
            "content": base64.b64encode(content).decode("ascii"),
            "branch": self.branch,
        }
        sha = self.current_sha()
        if sha is not None:
            body["sha"] = sha
        with self.opener(self._request("PUT", body=body), timeout=15) as response:
            payload = json.load(response)
        commit_sha = payload.get("commit", {}).get("sha")
        if not isinstance(commit_sha, str):
            raise ValueError("GitHub update response did not contain a commit SHA")
        return commit_sha


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Publish pipeline metrics to GitHub Pages")
    parser.add_argument("--post-database", type=Path, default=Path("data/posts.sqlite3"))
    parser.add_argument("--label-database", type=Path, default=Path("data/labels.sqlite3"))
    parser.add_argument("--config", type=Path, default=Path("config/spoiler_tags.json"))
    parser.add_argument("--repository", required=True)
    parser.add_argument("--path", default="docs/metrics.json")
    parser.add_argument("--branch", default="main")
    parser.add_argument("--token-env", default="GITHUB_METRICS_TOKEN")
    parser.add_argument("--interval-seconds", type=float, default=300.0)
    parser.add_argument("--window-seconds", type=int, default=300)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args(argv)

    post_queue = PostQueue(args.post_database)
    label_queue = LabelQueue(args.label_database)
    while True:
        token = os.environ.get(args.token_env)
        if not token:
            print(
                json.dumps(
                    {
                        "metrics_publish_error": (
                            f"environment variable {args.token_env} is not set"
                        )
                    }
                ),
                file=sys.stderr,
                flush=True,
            )
            if args.once:
                return 2
        else:
            try:
                snapshot = build_snapshot(
                    post_queue,
                    label_queue,
                    args.config,
                    window_seconds=args.window_seconds,
                )
                commit = GitHubContentsPublisher(
                    repository=args.repository,
                    path=args.path,
                    branch=args.branch,
                    token=token,
                ).publish(snapshot)
                print(
                    json.dumps(
                        {
                            "metrics_published": snapshot["generated_at"],
                            "commit": commit,
                        }
                    ),
                    flush=True,
                )
            except Exception as error:
                print(
                    json.dumps(
                        {
                            "metrics_publish_error": (
                                f"{type(error).__name__}: {error}"
                            )
                        }
                    ),
                    file=sys.stderr,
                    flush=True,
                )
                if args.once:
                    return 1
        if args.once:
            return 0
        time.sleep(args.interval_seconds)


if __name__ == "__main__":
    raise SystemExit(main())
