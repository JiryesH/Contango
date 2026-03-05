#!/usr/bin/env python3
"""
Incremental FinBERT sentiment scoring for commodity headlines.

This module can be:
1) Imported by rss_scraper.py to score articles in-memory.
2) Run standalone to score a JSON feed file.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

try:
    import torch
    from transformers import pipeline as hf_pipeline
    HAS_SENTIMENT_DEPS = True
except ImportError:
    HAS_SENTIMENT_DEPS = False
    torch = None
    hf_pipeline = None


SENTIMENT_LABEL_KEYS = ("positive", "negative", "neutral")


@dataclass
class SentimentConfig:
    enabled: bool = True
    model_name: str = "ProsusAI/finbert"
    batch_size: int = 32
    max_length: int = 128
    use_description: bool = False
    force_rescore: bool = False


def normalize_sentiment_scores(raw_scores: Any) -> dict[str, float]:
    """
    Normalize FinBERT output to always include all labels.
    Supports:
      - top_k=None output: [{"label": "...", "score": ...}, ...]
      - single top output: {"label": "...", "score": ...}
    """
    out = {k: 0.0 for k in SENTIMENT_LABEL_KEYS}

    if isinstance(raw_scores, dict):
        label = str(raw_scores.get("label", "")).lower()
        score = float(raw_scores.get("score", 0.0))
        if label in out:
            out[label] = score
        return out

    if isinstance(raw_scores, list):
        for item in raw_scores:
            label = str(item.get("label", "")).lower()
            score = float(item.get("score", 0.0))
            if label in out:
                out[label] = score
        return out

    return out


def pick_sentiment_label(scores: dict[str, float]) -> tuple[str, float]:
    label = max(scores, key=lambda k: scores[k])
    return label, float(scores[label])


def build_sentiment_text(article: dict[str, Any], use_description: bool) -> str:
    title = " ".join((article.get("title") or "").split())
    if not use_description:
        return title

    description = " ".join((article.get("description") or "").split())
    if not description:
        return title
    return f"{title}. {description}"


def sentiment_text_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


class FinBERTScorer:
    """Incremental FinBERT scorer that skips unchanged articles."""

    def __init__(self, config: SentimentConfig):
        self.config = config
        self._classifier = None
        self._input_mode = "title+description" if config.use_description else "title"
        self.logger = logging.getLogger("sentiment_finbert")

    def _ensure_classifier(self):
        if self._classifier is not None:
            return
        if not HAS_SENTIMENT_DEPS:
            raise RuntimeError(
                "FinBERT scoring requires transformers and torch. "
                "Install with: pip install -U transformers torch"
            )

        device = 0 if (torch is not None and torch.cuda.is_available()) else -1
        device_name = "cuda" if device == 0 else "cpu"
        self.logger.info(
            "Loading sentiment model '%s' on %s...",
            self.config.model_name,
            device_name,
        )

        self._classifier = hf_pipeline(
            task="text-classification",
            model=self.config.model_name,
            tokenizer=self.config.model_name,
            top_k=None,
            device=device,
        )

    def _needs_rescore(self, article: dict[str, Any], text_hash: str) -> bool:
        if self.config.force_rescore:
            return True

        sentiment = article.get("sentiment")
        if not isinstance(sentiment, dict):
            return True
        if sentiment.get("model") != self.config.model_name:
            return True
        if sentiment.get("input_mode") != self._input_mode:
            return True
        if sentiment.get("text_hash") != text_hash:
            return True
        probabilities = sentiment.get("probabilities")
        if not isinstance(probabilities, dict):
            return True
        return False

    def score_incremental(self, articles: list[dict]) -> dict[str, Any]:
        started = time.time()
        now_iso = datetime.now(timezone.utc).isoformat()

        stats = {
            "enabled": True,
            "model": self.config.model_name,
            "input_mode": self._input_mode,
            "candidate_articles": 0,
            "scored": 0,
            "reused": 0,
            "blank_text": 0,
            "errors": 0,
            "scored_at": now_iso,
        }

        to_score_idx = []
        to_score_text = []
        to_score_hash = []

        for idx, article in enumerate(articles):
            text = build_sentiment_text(article, self.config.use_description)
            if not text:
                stats["blank_text"] += 1
                continue

            text_hash = sentiment_text_hash(text)
            if self._needs_rescore(article, text_hash):
                to_score_idx.append(idx)
                to_score_text.append(text)
                to_score_hash.append(text_hash)
            else:
                stats["reused"] += 1

        stats["candidate_articles"] = len(to_score_idx)
        if not to_score_idx:
            stats["duration_ms"] = int((time.time() - started) * 1000)
            return stats

        self._ensure_classifier()
        results = self._classifier(
            to_score_text,
            batch_size=self.config.batch_size,
            truncation=True,
            max_length=self.config.max_length,
        )

        if isinstance(results, dict):
            results = [results]

        for idx, raw_scores, text_hash in zip(to_score_idx, results, to_score_hash):
            probs = normalize_sentiment_scores(raw_scores)
            top_label, confidence = pick_sentiment_label(probs)
            compound = float(probs["positive"] - probs["negative"])

            articles[idx]["sentiment"] = {
                "label": top_label,
                "confidence": confidence,
                "probabilities": probs,
                "compound": compound,
                "model": self.config.model_name,
                "input_mode": self._input_mode,
                "text_hash": text_hash,
                "scored_at": now_iso,
            }

        stats["scored"] = len(to_score_idx)
        stats["duration_ms"] = int((time.time() - started) * 1000)
        return stats


def log_sentiment_rollup(
    articles: list[dict],
    logger: Optional[logging.Logger] = None,
) -> None:
    """Log a compact rollup of average compound score by feed + category."""
    log = logger or logging.getLogger("sentiment_finbert")

    buckets: dict[tuple[str, str], list[float]] = {}
    for article in articles:
        sentiment = article.get("sentiment") or {}
        compound = sentiment.get("compound")
        if compound is None:
            continue
        key = (
            str(article.get("feed") or "Unknown"),
            str(article.get("category") or "Unknown"),
        )
        buckets.setdefault(key, []).append(float(compound))

    if not buckets:
        log.info("No sentiment scores to summarize.")
        return

    log.info("Sentiment rollup (avg compound by feed/category):")
    rows = sorted(
        ((k[0], k[1], sum(v) / len(v), len(v)) for k, v in buckets.items()),
        key=lambda row: (row[0], row[1]),
    )
    for feed, category, avg_compound, count in rows:
        log.info(
            "  %s | %-25s | avg=%+.3f | n=%s",
            feed,
            category,
            avg_compound,
            count,
        )


def load_feed(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_feed(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run incremental FinBERT sentiment scoring on feed JSON."
    )
    parser.add_argument("--input", required=True, help="Path to input feed JSON.")
    parser.add_argument(
        "--output",
        required=True,
        help="Path to output JSON (can be same as --input).",
    )
    parser.add_argument("--model", default="ProsusAI/finbert")
    parser.add_argument("--batch-size", type=int, default=32)
    parser.add_argument("--max-length", type=int, default=128)
    parser.add_argument(
        "--use-description",
        action="store_true",
        help="Score title + description instead of title only.",
    )
    parser.add_argument(
        "--force-rescore",
        action="store_true",
        help="Rescore all non-empty items even if cached sentiment exists.",
    )
    args = parser.parse_args()

    data = load_feed(Path(args.input))
    articles = data.get("articles") or []
    if not isinstance(articles, list):
        raise ValueError("Input JSON does not contain a list at key 'articles'.")

    config = SentimentConfig(
        enabled=True,
        model_name=args.model,
        batch_size=args.batch_size,
        max_length=args.max_length,
        use_description=args.use_description,
        force_rescore=args.force_rescore,
    )

    scorer = FinBERTScorer(config)
    stats = scorer.score_incremental(articles)

    metadata = data.get("metadata")
    if not isinstance(metadata, dict):
        metadata = {}
        data["metadata"] = metadata
    metadata["sentiment"] = stats

    save_feed(Path(args.output), data)
    print(
        f"Wrote: {args.output} "
        f"(scored={stats.get('scored', 0)}, reused={stats.get('reused', 0)})"
    )
    log_sentiment_rollup(articles)


if __name__ == "__main__":
    main()
