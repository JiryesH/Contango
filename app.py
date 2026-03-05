#!/usr/bin/env python3
"""
Control API for orchestration of project scripts.

Run:
    python app.py --host 127.0.0.1 --port 8081

Endpoints:
    GET  /api/health
    GET  /api/jobs
    GET  /api/jobs/<id>
    POST /api/jobs/scrape
    POST /api/jobs/sentiment
    POST /api/jobs/ner
    POST /api/jobs/pipeline

All POST endpoints accept JSON bodies and return a job object immediately.
Jobs run in a background thread; poll /api/jobs/<id> for completion.
"""

from __future__ import annotations

import argparse
import json
import threading
import traceback
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from itertools import count
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlparse

import rss_scraper
import sentiment_finbert
import ner_spacy
from ner_spacy import NERConfig, SpacyNERExtractor, log_ner_rollup
from sentiment_finbert import SentimentConfig, FinBERTScorer, log_sentiment_rollup


ROOT_DIR = Path(__file__).resolve().parent
JOBS: dict[str, dict[str, Any]] = {}
JOB_COUNTER = count(1)
JOBS_LOCK = threading.Lock()
RUNNER_LOCK = threading.Lock()  # single writer lock around feed updates


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def resolve_path(raw_path: str | None, fallback: Path) -> Path:
    if not raw_path:
        return fallback
    p = Path(raw_path)
    if not p.is_absolute():
        p = ROOT_DIR / p
    return p


def to_int(value: Any, default: int) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except Exception:
        return default


def build_sentiment_config(payload: dict[str, Any], enabled: bool) -> SentimentConfig:
    return SentimentConfig(
        enabled=enabled,
        model_name=str(payload.get("sentiment_model", "ProsusAI/finbert")),
        batch_size=to_int(payload.get("sentiment_batch_size"), 32),
        max_length=to_int(payload.get("sentiment_max_length"), 128),
        use_description=bool(payload.get("sentiment_use_description", False)),
        force_rescore=bool(payload.get("sentiment_force_rescore", False)),
    )


def build_ner_config(payload: dict[str, Any], enabled: bool) -> NERConfig:
    return NERConfig(
        enabled=enabled,
        model_name=str(payload.get("ner_model", "en_core_web_lg")),
        batch_size=to_int(payload.get("ner_batch_size"), 64),
        use_description=bool(payload.get("ner_use_description", False)),
        force_rescore=bool(payload.get("ner_force_rescore", False)),
        max_entities=to_int(payload.get("ner_max_entities"), 18),
    )


def run_scrape_job(payload: dict[str, Any]) -> dict[str, Any]:
    sentiment_enabled = bool(payload.get("sentiment", False))
    ner_enabled = bool(payload.get("ner", False))
    sentiment_config = (
        build_sentiment_config(payload, enabled=True) if sentiment_enabled else None
    )
    ner_config = build_ner_config(payload, enabled=True) if ner_enabled else None

    articles, stats, sentiment_stats, ner_stats = rss_scraper.run_once(
        sentiment_config=sentiment_config,
        ner_config=ner_config,
    )

    return {
        "article_count": len(articles),
        "fetch": stats,
        "sentiment": sentiment_stats,
        "ner": ner_stats,
        "feed_path": str(rss_scraper.OUTPUT_FILE),
    }


def run_pipeline_job(payload: dict[str, Any]) -> dict[str, Any]:
    merged = dict(payload)
    merged.setdefault("sentiment", True)
    merged.setdefault("ner", True)
    return run_scrape_job(merged)


def run_sentiment_job(payload: dict[str, Any]) -> dict[str, Any]:
    input_path = resolve_path(payload.get("input"), rss_scraper.OUTPUT_FILE)
    output_path = resolve_path(payload.get("output"), input_path)

    data = sentiment_finbert.load_feed(input_path)
    articles = data.get("articles") or []
    if not isinstance(articles, list):
        raise ValueError("Input JSON does not contain a list at key 'articles'.")

    config = SentimentConfig(
        enabled=True,
        model_name=str(payload.get("model", "ProsusAI/finbert")),
        batch_size=to_int(payload.get("batch_size"), 32),
        max_length=to_int(payload.get("max_length"), 128),
        use_description=bool(payload.get("use_description", False)),
        force_rescore=bool(payload.get("force_rescore", False)),
    )

    scorer = FinBERTScorer(config)
    stats = scorer.score_incremental(articles)

    metadata = data.get("metadata")
    if not isinstance(metadata, dict):
        metadata = {}
        data["metadata"] = metadata
    metadata["sentiment"] = stats

    sentiment_finbert.save_feed(output_path, data)
    if stats.get("scored", 0) > 0:
        log_sentiment_rollup(articles)

    return {
        "article_count": len(articles),
        "sentiment": stats,
        "input_path": str(input_path),
        "output_path": str(output_path),
    }


def run_ner_job(payload: dict[str, Any]) -> dict[str, Any]:
    input_path = resolve_path(payload.get("input"), rss_scraper.OUTPUT_FILE)
    output_path = resolve_path(payload.get("output"), input_path)

    data = ner_spacy.load_feed(input_path)
    articles = data.get("articles") or []
    if not isinstance(articles, list):
        raise ValueError("Input JSON does not contain a list at key 'articles'.")

    config = NERConfig(
        enabled=True,
        model_name=str(payload.get("model", "en_core_web_lg")),
        batch_size=to_int(payload.get("batch_size"), 64),
        use_description=bool(payload.get("use_description", False)),
        force_rescore=bool(payload.get("force_rescore", False)),
        max_entities=to_int(payload.get("max_entities"), 18),
    )

    extractor = SpacyNERExtractor(config)
    stats = extractor.extract_incremental(articles)

    metadata = data.get("metadata")
    if not isinstance(metadata, dict):
        metadata = {}
        data["metadata"] = metadata
    metadata["ner"] = stats

    ner_spacy.save_feed(output_path, data)
    if stats.get("extracted", 0) > 0:
        log_ner_rollup(articles)

    return {
        "article_count": len(articles),
        "ner": stats,
        "input_path": str(input_path),
        "output_path": str(output_path),
    }


def job_summary(job: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": job["id"],
        "kind": job["kind"],
        "status": job["status"],
        "created_at": job["created_at"],
        "started_at": job.get("started_at"),
        "finished_at": job.get("finished_at"),
    }


def job_detail(job: dict[str, Any]) -> dict[str, Any]:
    detail = dict(job_summary(job))
    if "result" in job:
        detail["result"] = job["result"]
    if "error" in job:
        detail["error"] = job["error"]
    return detail


def submit_job(kind: str, payload: dict[str, Any], fn: Callable[[dict[str, Any]], dict[str, Any]]) -> dict[str, Any]:
    job_id = str(next(JOB_COUNTER))
    job = {
        "id": job_id,
        "kind": kind,
        "status": "queued",
        "created_at": utc_now_iso(),
        "payload": payload,
    }
    with JOBS_LOCK:
        JOBS[job_id] = job

    thread = threading.Thread(
        target=run_job_worker,
        args=(job_id, payload, fn),
        daemon=True,
    )
    thread.start()
    return job


def run_job_worker(job_id: str, payload: dict[str, Any], fn: Callable[[dict[str, Any]], dict[str, Any]]) -> None:
    with JOBS_LOCK:
        job = JOBS[job_id]
        job["status"] = "running"
        job["started_at"] = utc_now_iso()

    try:
        with RUNNER_LOCK:
            result = fn(payload)
        with JOBS_LOCK:
            job = JOBS[job_id]
            job["status"] = "succeeded"
            job["result"] = result
            job["finished_at"] = utc_now_iso()
    except Exception as exc:
        with JOBS_LOCK:
            job = JOBS[job_id]
            job["status"] = "failed"
            job["error"] = {
                "message": str(exc),
                "traceback": traceback.format_exc(limit=10),
            }
            job["finished_at"] = utc_now_iso()


class ApiHandler(BaseHTTPRequestHandler):
    server_version = "ContangoControlAPI/1.0"

    def _json_response(self, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()
        self.wfile.write(body)

    def _read_json_body(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0") or 0)
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        data = json.loads(raw.decode("utf-8"))
        if not isinstance(data, dict):
            raise ValueError("JSON body must be an object.")
        return data

    def do_OPTIONS(self) -> None:  # noqa: N802
        self._json_response(HTTPStatus.OK, {"ok": True})

    def do_GET(self) -> None:  # noqa: N802
        path = urlparse(self.path).path

        if path == "/api/health":
            with JOBS_LOCK:
                queued = sum(1 for j in JOBS.values() if j["status"] == "queued")
                running = sum(1 for j in JOBS.values() if j["status"] == "running")
            self._json_response(
                HTTPStatus.OK,
                {
                    "ok": True,
                    "time": utc_now_iso(),
                    "jobs": {"queued": queued, "running": running},
                },
            )
            return

        if path == "/api/jobs":
            with JOBS_LOCK:
                jobs = sorted(JOBS.values(), key=lambda j: int(j["id"]), reverse=True)
                payload = [job_summary(job) for job in jobs]
            self._json_response(HTTPStatus.OK, {"jobs": payload})
            return

        if path.startswith("/api/jobs/"):
            job_id = path.rsplit("/", 1)[-1]
            with JOBS_LOCK:
                job = JOBS.get(job_id)
            if not job:
                self._json_response(HTTPStatus.NOT_FOUND, {"error": "Job not found."})
                return
            self._json_response(HTTPStatus.OK, {"job": job_detail(job)})
            return

        self._json_response(HTTPStatus.NOT_FOUND, {"error": "Route not found."})

    def do_POST(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        try:
            payload = self._read_json_body()
        except Exception as exc:
            self._json_response(
                HTTPStatus.BAD_REQUEST,
                {"error": f"Invalid JSON body: {exc}"},
            )
            return

        if path == "/api/jobs/scrape":
            job = submit_job("scrape", payload, run_scrape_job)
            self._json_response(HTTPStatus.ACCEPTED, {"job": job_summary(job)})
            return

        if path == "/api/jobs/sentiment":
            job = submit_job("sentiment", payload, run_sentiment_job)
            self._json_response(HTTPStatus.ACCEPTED, {"job": job_summary(job)})
            return

        if path == "/api/jobs/ner":
            job = submit_job("ner", payload, run_ner_job)
            self._json_response(HTTPStatus.ACCEPTED, {"job": job_summary(job)})
            return

        if path == "/api/jobs/pipeline":
            job = submit_job("pipeline", payload, run_pipeline_job)
            self._json_response(HTTPStatus.ACCEPTED, {"job": job_summary(job)})
            return

        self._json_response(HTTPStatus.NOT_FOUND, {"error": "Route not found."})

    def log_message(self, format: str, *args: Any) -> None:
        # Keep terminal noise low.
        return


def main() -> None:
    parser = argparse.ArgumentParser(description="Contango script control API")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8081)
    args = parser.parse_args()

    server = ThreadingHTTPServer((args.host, args.port), ApiHandler)
    print(f"Control API listening on http://{args.host}:{args.port}")
    print("Routes: GET /api/health, GET /api/jobs, GET /api/jobs/<id>,")
    print(
        "        POST /api/jobs/scrape, /api/jobs/sentiment, /api/jobs/ner, /api/jobs/pipeline"
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
