#!/usr/bin/env python3
"""
Incremental spaCy NER extraction for commodity headlines.

This module can be:
1) Imported by rss_scraper.py to extract entities in-memory.
2) Run standalone to enrich a JSON feed file.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

try:
    import spacy

    HAS_NER_DEPS = True
except ImportError:
    HAS_NER_DEPS = False
    spacy = None


COUNTRY_ENTITY_LABELS = {"GPE", "LOC"}


@dataclass
class NERConfig:
    enabled: bool = True
    model_name: str = "en_core_web_lg"
    batch_size: int = 64
    use_description: bool = False
    force_rescore: bool = False
    max_entities: int = 18


def build_ner_text(article: dict[str, Any], use_description: bool) -> str:
    title = " ".join((article.get("title") or "").split())
    if not use_description:
        return title

    description = " ".join((article.get("description") or "").split())
    if not description:
        return title
    return f"{title}. {description}"


def ner_text_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def unique_in_order(items: list[tuple[str, str]]) -> list[tuple[str, str]]:
    seen = set()
    out: list[tuple[str, str]] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _normalize_country_key(value: str) -> str:
    compact = re.sub(r"\s+", " ", (value or "").strip())
    compact = compact.replace(".", "")
    return compact.lower()


def try_country_matcher() -> Optional[Callable[[str], Optional[str]]]:
    try:
        import pycountry  # type: ignore
    except Exception:
        return None

    aliases = {
        "u.s": "United States",
        "us": "United States",
        "usa": "United States",
        "u.a.e": "United Arab Emirates",
        "uae": "United Arab Emirates",
        "uk": "United Kingdom",
        "britain": "United Kingdom",
        "russia": "Russian Federation",
        "south korea": "Korea, Republic of",
        "north korea": "Korea, Democratic People's Republic of",
    }

    display_overrides = {
        "Iran, Islamic Republic of": "Iran",
        "Korea, Republic of": "South Korea",
        "Korea, Democratic People's Republic of": "North Korea",
        "Russian Federation": "Russia",
        "Syrian Arab Republic": "Syria",
        "Venezuela, Bolivarian Republic of": "Venezuela",
        "Bolivia, Plurinational State of": "Bolivia",
        "Moldova, Republic of": "Moldova",
        "Tanzania, United Republic of": "Tanzania",
        "Taiwan, Province of China": "Taiwan",
        "Viet Nam": "Vietnam",
        "Lao People's Democratic Republic": "Laos",
        "Brunei Darussalam": "Brunei",
    }

    by_name: dict[str, str] = {}
    for country in list(pycountry.countries):
        canonical = getattr(country, "name", "")
        if not canonical:
            continue
        for candidate in [
            canonical,
            getattr(country, "official_name", None),
            getattr(country, "common_name", None),
        ]:
            if not candidate:
                continue
            by_name[_normalize_country_key(str(candidate))] = canonical

    def to_country(ent_text: str) -> Optional[str]:
        raw = re.sub(r"\s+", " ", (ent_text or "").strip())
        if not raw:
            return None

        aliased = aliases.get(_normalize_country_key(raw), raw)
        key = _normalize_country_key(aliased)

        direct = by_name.get(key)
        if direct:
            return display_overrides.get(direct, direct)

        if len(aliased) in (2, 3) and aliased.isalpha():
            try:
                hit = pycountry.countries.get(alpha_2=aliased.upper())
                if not hit:
                    hit = pycountry.countries.get(alpha_3=aliased.upper())
                if hit:
                    name = str(hit.name)
                    return display_overrides.get(name, name)
            except Exception:
                pass

        try:
            hits = pycountry.countries.search_fuzzy(aliased)
            if hits:
                name = str(hits[0].name)
                return display_overrides.get(name, name)
        except Exception:
            return None

        return None

    return to_country


class SpacyNERExtractor:
    """Incremental spaCy NER extractor that skips unchanged articles."""

    def __init__(self, config: NERConfig):
        self.config = config
        self._nlp = None
        self._country_matcher = try_country_matcher()
        self._input_mode = "title+description" if config.use_description else "title"
        self.logger = logging.getLogger("ner_spacy")

    def _ensure_nlp(self):
        if self._nlp is not None:
            return

        if not HAS_NER_DEPS:
            raise RuntimeError(
                "spaCy NER requires spacy and a model package. "
                "Install with: pip install -U spacy && python -m spacy download en_core_web_lg"
            )

        self.logger.info("Loading NER model '%s'...", self.config.model_name)
        self._nlp = spacy.load(self.config.model_name)

    def _needs_refresh(self, article: dict[str, Any], text_hash: str) -> bool:
        if self.config.force_rescore:
            return True

        ner = article.get("ner")
        if not isinstance(ner, dict):
            return True
        if ner.get("model") != self.config.model_name:
            return True
        if ner.get("input_mode") != self._input_mode:
            return True
        if ner.get("text_hash") != text_hash:
            return True
        if not isinstance(ner.get("countries"), list):
            return True
        if not isinstance(ner.get("entities"), list):
            return True
        return False

    def _extract_from_doc(self, doc) -> tuple[list[dict[str, str]], list[str]]:
        raw_entities: list[tuple[str, str]] = []
        for ent in doc.ents:
            text = str(ent.text or "").strip()
            label = str(ent.label_ or "").strip()
            if not text or not label:
                continue
            raw_entities.append((text, label))

        raw_entities = unique_in_order(raw_entities)
        if self.config.max_entities > 0:
            raw_entities = raw_entities[: self.config.max_entities]

        entities = [{"text": text, "label": label} for text, label in raw_entities]

        countries: list[str] = []
        for text, label in raw_entities:
            if label not in COUNTRY_ENTITY_LABELS:
                continue

            if self._country_matcher is not None:
                normalized = self._country_matcher(text)
                if normalized and normalized not in countries:
                    countries.append(normalized)
            else:
                if text not in countries:
                    countries.append(text)

        return entities, countries

    def extract_incremental(self, articles: list[dict]) -> dict[str, Any]:
        started = time.time()
        now_iso = datetime.now(timezone.utc).isoformat()

        stats: dict[str, Any] = {
            "enabled": True,
            "model": self.config.model_name,
            "input_mode": self._input_mode,
            "candidate_articles": 0,
            "extracted": 0,
            "reused": 0,
            "blank_text": 0,
            "errors": 0,
            "extracted_at": now_iso,
        }

        to_extract_idx: list[int] = []
        to_extract_text: list[str] = []
        to_extract_hash: list[str] = []

        for idx, article in enumerate(articles):
            text = build_ner_text(article, self.config.use_description)
            if not text:
                stats["blank_text"] += 1
                continue

            text_hash = ner_text_hash(text)
            if self._needs_refresh(article, text_hash):
                to_extract_idx.append(idx)
                to_extract_text.append(text)
                to_extract_hash.append(text_hash)
            else:
                stats["reused"] += 1

        stats["candidate_articles"] = len(to_extract_idx)
        if not to_extract_idx:
            stats["duration_ms"] = int((time.time() - started) * 1000)
            return stats

        self._ensure_nlp()
        docs = self._nlp.pipe(to_extract_text, batch_size=self.config.batch_size)

        for idx, text_hash, doc in zip(to_extract_idx, to_extract_hash, docs):
            try:
                entities, countries = self._extract_from_doc(doc)
                articles[idx]["ner"] = {
                    "entities": entities,
                    "countries": countries,
                    "model": self.config.model_name,
                    "input_mode": self._input_mode,
                    "text_hash": text_hash,
                    "extracted_at": now_iso,
                }
                stats["extracted"] += 1
            except Exception:
                stats["errors"] += 1

        stats["duration_ms"] = int((time.time() - started) * 1000)
        return stats


def log_ner_rollup(
    articles: list[dict],
    logger: Optional[logging.Logger] = None,
) -> None:
    """Log a compact rollup of the most-mentioned countries."""
    log = logger or logging.getLogger("ner_spacy")

    counts: dict[str, int] = {}
    for article in articles:
        ner = article.get("ner") or {}
        countries = ner.get("countries")
        if not isinstance(countries, list):
            continue
        for country in countries:
            value = str(country or "").strip()
            if not value:
                continue
            counts[value] = counts.get(value, 0) + 1

    if not counts:
        log.info("NER rollup: no country mentions found.")
        return

    log.info("NER rollup (top country mentions):")
    top = sorted(counts.items(), key=lambda item: item[1], reverse=True)[:10]
    for country, count in top:
        log.info("  %s: %s", country, count)


def load_feed(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_feed(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run incremental spaCy NER extraction on feed JSON."
    )
    parser.add_argument("--input", required=True, help="Path to input feed JSON.")
    parser.add_argument(
        "--output",
        required=True,
        help="Path to output JSON (can be same as --input).",
    )
    parser.add_argument("--model", default="en_core_web_lg")
    parser.add_argument("--batch-size", type=int, default=64)
    parser.add_argument(
        "--use-description",
        action="store_true",
        help="Extract from title + description instead of title only.",
    )
    parser.add_argument(
        "--force-rescore",
        action="store_true",
        help="Re-extract all non-empty items even if cached NER exists.",
    )
    parser.add_argument(
        "--max-entities",
        type=int,
        default=18,
        help="Maximum stored entities per article (default: 18).",
    )
    args = parser.parse_args()

    data = load_feed(Path(args.input))
    articles = data.get("articles") or []
    if not isinstance(articles, list):
        raise ValueError("Input JSON does not contain a list at key 'articles'.")

    config = NERConfig(
        enabled=True,
        model_name=args.model,
        batch_size=args.batch_size,
        use_description=args.use_description,
        force_rescore=args.force_rescore,
        max_entities=args.max_entities,
    )

    extractor = SpacyNERExtractor(config)
    stats = extractor.extract_incremental(articles)

    metadata = data.get("metadata")
    if not isinstance(metadata, dict):
        metadata = {}
        data["metadata"] = metadata
    metadata["ner"] = stats

    save_feed(Path(args.output), data)
    print(
        f"Wrote: {args.output} "
        f"(extracted={stats.get('extracted', 0)}, reused={stats.get('reused', 0)})"
    )
    log_ner_rollup(articles)


if __name__ == "__main__":
    main()
