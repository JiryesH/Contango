"""
Commodity News Feed - RSS Scraper
==================================
Scrapes and normalizes RSS feeds from ICIS and S&P Global Commodity Insights,
joining them into a single unified JSON data file for the frontend to consume.

Usage:
    python rss_scraper.py                # Run once, output to data/feed.json
    python rss_scraper.py --daemon       # Run continuously, polling every 10 minutes
    python rss_scraper.py --daemon --interval 300  # Custom poll interval (seconds)
    python rss_scraper.py --sentiment    # Also score headlines with FinBERT
    python rss_scraper.py --ner          # Also extract entities/countries with spaCy
    python sentiment_finbert.py --input data/feed.json --output data/feed.json
    python ner_spacy.py --input data/feed.json --output data/feed.json
    python app.py --host 127.0.0.1 --port 8081  # Control API for job orchestration

Requirements:
    pip install feedparser requests curl_cffi
    (curl_cffi is needed to bypass Akamai bot detection on S&P Global feeds)
    Optional: pip install cloudscraper  (additional fallback)
    Optional: pip install transformers torch  (for sentiment_finbert.py / --sentiment)
    Optional: pip install spacy pycountry  (for ner_spacy.py / --ner)
              python -m spacy download en_core_web_lg
"""

import feedparser
import requests
import json
import hashlib
import os
import sys
import time
import argparse
import logging
import threading
import subprocess
import shutil
import http.server
import socketserver
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Optional, Any
from classifier import classify_category

# Sentiment and NER are optional — they require heavy ML deps (torch, spaCy)
# that are not installed in lightweight environments (e.g. GitHub Actions).
# When unavailable, stub classes keep the rest of the code path intact while
# the --sentiment / --ner flags simply have no effect.
try:
    from sentiment_finbert import SentimentConfig, FinBERTScorer, log_sentiment_rollup
    HAS_SENTIMENT = True
except ImportError:
    HAS_SENTIMENT = False
    from dataclasses import dataclass as _dc

    @_dc
    class SentimentConfig:  # type: ignore[no-redef]
        enabled: bool = False
        model_name: str = "ProsusAI/finbert"
        batch_size: int = 32
        max_length: int = 128
        use_description: bool = False
        force_rescore: bool = False

    class FinBERTScorer:  # type: ignore[no-redef]
        def __init__(self, config): pass

    def log_sentiment_rollup(articles): pass  # type: ignore[misc]

try:
    from ner_spacy import NERConfig, SpacyNERExtractor, log_ner_rollup
    HAS_NER = True
except ImportError:
    HAS_NER = False
    from dataclasses import dataclass as _dc2

    @_dc2
    class NERConfig:  # type: ignore[no-redef]
        enabled: bool = False
        model_name: str = "en_core_web_lg"
        batch_size: int = 64
        use_description: bool = False
        force_rescore: bool = False
        max_entities: int = 18

    class SpacyNERExtractor:  # type: ignore[no-redef]
        def __init__(self, config): pass

    def log_ner_rollup(articles): pass  # type: ignore[misc]

# Try to import curl_cffi (best TLS fingerprint impersonation — beats Akamai)
try:
    from curl_cffi import requests as cffi_requests
    HAS_CURL_CFFI = True
except ImportError:
    HAS_CURL_CFFI = False

# Try to import cloudscraper (handles Cloudflare JS challenges, sometimes Akamai)
try:
    import cloudscraper
    _scraper_session = cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "darwin", "mobile": False}
    )
    HAS_CLOUDSCRAPER = True
except ImportError:
    HAS_CLOUDSCRAPER = False
    _scraper_session = None

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

FEEDS = {
    # ICIS
    "ICIS": {
        "url": "https://www.icis.com/rss/publicrss/",
        "source": "ICIS",
        "category": "General",
    },
    # S&P Global Energy
    # Note: the top-level /rss/oil.xml feed was abandoned by S&P and has data
    # from 2023. Use the more specific crude and refined feeds instead.
    "S&P Oil - Crude": {
        "url": "https://www.spglobal.com/content/spglobal/energy/us/en/rss/oil-crude.xml",
        "source": "S&P Global",
        "category": "Oil - Crude",
    },
    "S&P Oil - Refined Products": {
        "url": "https://www.spglobal.com/content/spglobal/energy/us/en/rss/oil-refined-products.xml",
        "source": "S&P Global",
        "category": "Oil - Refined Products",
    },
    "S&P Fertilizers": {
        "url": "https://www.spglobal.com/content/spglobal/energy/us/en/rss/fertilizers.xml",
        "source": "S&P Global",
        "category": "Fertilizers",
    },
    "S&P Electric Power": {
        "url": "https://www.spglobal.com/content/spglobal/energy/us/en/rss/electric-power.xml",
        "source": "S&P Global",
        "category": "Electric Power",
    },
    "S&P Natural Gas": {
        "url": "https://www.spglobal.com/content/spglobal/energy/us/en/rss/natural-gas.xml",
        "source": "S&P Global",
        "category": "Natural Gas",
    },
    "S&P Coal": {
        "url": "https://www.spglobal.com/content/spglobal/energy/us/en/rss/coal.xml",
        "source": "S&P Global",
        "category": "Coal",
    },
    "S&P Chemicals": {
        "url": "https://www.spglobal.com/content/spglobal/energy/us/en/rss/chemicals.xml",
        "source": "S&P Global",
        "category": "Chemicals",
    },
    "S&P Metals": {
        "url": "https://www.spglobal.com/content/spglobal/energy/us/en/rss/metals.xml",
        "source": "S&P Global",
        "category": "Metals",
    },
    "S&P Shipping": {
        "url": "https://www.spglobal.com/content/spglobal/energy/us/en/rss/shipping.xml",
        "source": "S&P Global",
        "category": "Shipping",
    },
    "S&P Agriculture": {
        "url": "https://www.spglobal.com/content/spglobal/energy/us/en/rss/agriculture.xml",
        "source": "S&P Global",
        "category": "Agriculture",
    },
    "S&P LNG": {
        "url": "https://www.spglobal.com/content/spglobal/energy/us/en/rss/lng.xml",
        "source": "S&P Global",
        "category": "LNG",
    },
    "S&P Energy Transition": {
        "url": "https://www.spglobal.com/content/spglobal/energy/us/en/rss/energy-transition.xml",
        "source": "S&P Global",
        "category": "Energy Transition",
    },
}

# Browser-like headers to avoid bot blocking (Akamai CDN on S&P Global)
REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Cache-Control": "max-age=0",
}

REQUEST_TIMEOUT = 15  # seconds

# Output
DATA_DIR = Path(__file__).parent / "data"
OUTPUT_FILE = DATA_DIR / "feed.json"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("rss_scraper")

# ---------------------------------------------------------------------------
# Date parsing
# ---------------------------------------------------------------------------

def parse_pub_date(raw: str) -> Optional[str]:
    """
    Parse various RSS date formats into a consistent ISO 8601 string (UTC).

    Handles:
      - Full RFC 822:  "Wed, 04 Mar 2026 09:37:00 GMT"
      - ICIS quirk:    "Wed, 04 Mar 2026 09:37"  (no seconds, no timezone)
      - feedparser's time.struct_time via published_parsed
    """
    if not raw:
        return None

    raw = raw.strip()

    # Try standard RFC 822 first
    try:
        dt = parsedate_to_datetime(raw)
        # If no timezone info, assume UTC
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        pass

    # ICIS format: "Wed, 04 Mar 2026 09:37" (no seconds)
    for fmt in [
        "%a, %d %b %Y %H:%M",
        "%a, %d %b %Y %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S",
    ]:
        try:
            dt = datetime.strptime(raw, fmt)
            dt = dt.replace(tzinfo=timezone.utc)
            return dt.isoformat()
        except ValueError:
            continue

    logger.warning(f"Could not parse date: '{raw}'")
    return raw  # Return raw string as fallback


def parse_pub_date_from_struct(struct_time) -> Optional[str]:
    """Convert feedparser's struct_time to ISO 8601 UTC string."""
    if struct_time is None:
        return None
    try:
        dt = datetime(*struct_time[:6], tzinfo=timezone.utc)
        return dt.isoformat()
    except Exception:
        return None

# ---------------------------------------------------------------------------
# Article ID generation
# ---------------------------------------------------------------------------

def generate_article_id(link: str, title: str) -> str:
    """
    Generate a stable, unique ID for deduplication.
    Uses the article link as primary key; falls back to title hash.
    """
    key = link if link else title
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]

# ---------------------------------------------------------------------------
# HTTP fetching — multi-tier strategy for Akamai-protected sites
# ---------------------------------------------------------------------------

def _fetch_with_requests(url: str) -> Optional[str]:
    """Tier 1: plain requests — works for ICIS and non-protected feeds."""
    try:
        resp = requests.get(url, headers=REQUEST_HEADERS, timeout=REQUEST_TIMEOUT)
        if resp.status_code == 200:
            return resp.text
        logger.debug(f"  requests → HTTP {resp.status_code}")
    except Exception as e:
        logger.debug(f"  requests → {e}")
    return None


def _fetch_with_curl_cffi(url: str) -> Optional[str]:
    """Tier 2: curl_cffi — impersonates Chrome's TLS fingerprint. Beats Akamai."""
    if not HAS_CURL_CFFI:
        return None
    try:
        resp = cffi_requests.get(url, impersonate="chrome", timeout=REQUEST_TIMEOUT)
        if resp.status_code == 200:
            return resp.text
        logger.debug(f"  curl_cffi → HTTP {resp.status_code}")
    except Exception as e:
        logger.debug(f"  curl_cffi → {e}")
    return None


def _fetch_with_cloudscraper(url: str) -> Optional[str]:
    """Tier 3: cloudscraper — handles Cloudflare JS challenges, sometimes Akamai."""
    if not HAS_CLOUDSCRAPER or _scraper_session is None:
        return None
    try:
        resp = _scraper_session.get(url, timeout=REQUEST_TIMEOUT)
        if resp.status_code == 200:
            return resp.text
        logger.debug(f"  cloudscraper → HTTP {resp.status_code}")
    except Exception as e:
        logger.debug(f"  cloudscraper → {e}")
    return None


def _fetch_with_curl(url: str) -> Optional[str]:
    """
    Tier 3: macOS/system curl — its TLS stack (SecureTransport / BoringSSL)
    has a different fingerprint than Python's, which often passes Akamai.
    """
    curl_path = shutil.which("curl")
    if not curl_path:
        return None
    try:
        result = subprocess.run(
            [
                curl_path,
                "-sS",                        # silent but show errors
                "--compressed",                # accept gzip/br
                "-L",                          # follow redirects
                "--max-time", str(REQUEST_TIMEOUT),
                "-H", f"User-Agent: {REQUEST_HEADERS['User-Agent']}",
                "-H", f"Accept: {REQUEST_HEADERS['Accept']}",
                "-H", f"Accept-Language: {REQUEST_HEADERS['Accept-Language']}",
                url,
            ],
            capture_output=True,
            text=True,
            timeout=REQUEST_TIMEOUT + 5,
        )
        if result.returncode == 0 and result.stdout and "<rss" in result.stdout[:500].lower():
            return result.stdout
        logger.debug(f"  curl → exit {result.returncode}, len={len(result.stdout or '')}")
    except Exception as e:
        logger.debug(f"  curl → {e}")
    return None


def fetch_url(url: str, feed_name: str) -> Optional[str]:
    """
    Try multiple HTTP strategies in order of preference.
    Returns the raw response body (XML text) or None.
    """
    # Tier 1: plain requests (fast, works for ICIS and non-protected feeds)
    body = _fetch_with_requests(url)
    if body:
        return body

    # Tier 2: curl_cffi (Chrome TLS impersonation — beats Akamai)
    body = _fetch_with_curl_cffi(url)
    if body:
        logger.debug(f"[{feed_name}] succeeded via curl_cffi")
        return body

    # Tier 3: cloudscraper (handles Cloudflare, sometimes Akamai)
    body = _fetch_with_cloudscraper(url)
    if body:
        logger.debug(f"[{feed_name}] succeeded via cloudscraper")
        return body

    # Tier 4: system curl (macOS curl has different TLS fingerprint)
    body = _fetch_with_curl(url)
    if body:
        logger.debug(f"[{feed_name}] succeeded via curl")
        return body

    return None


# ---------------------------------------------------------------------------
# Feed fetching & parsing
# ---------------------------------------------------------------------------

def parse_feed_entries(xml_text: str, feed_name: str, feed_config: dict) -> list[dict]:
    """Parse RSS XML into a list of normalized article dicts."""
    source = feed_config["source"]
    category = feed_config["category"]

    feed = feedparser.parse(xml_text)

    if feed.bozo and not feed.entries:
        logger.warning(f"[{feed_name}] Feed parse error: {feed.bozo_exception}")
        return []

    articles = []
    for entry in feed.entries:
        title = " ".join(entry.get("title", "").split())
        link = entry.get("link", "").strip()
        description = " ".join(
            entry.get("summary", entry.get("description", "")).split()
        )

        raw_date = entry.get("published", entry.get("updated", ""))
        iso_date = parse_pub_date(raw_date)
        if iso_date is None or iso_date == raw_date:
            iso_date = parse_pub_date_from_struct(
                entry.get("published_parsed", entry.get("updated_parsed"))
            ) or iso_date

        # Classify ICIS articles ("General") using commodity taxonomy keywords
        article_category = category
        if category == "General":
            classified = classify_category(title, description)
            if classified:
                article_category = classified

        articles.append({
            "id": generate_article_id(link, title),
            "title": title,
            "description": description,
            "link": link,
            "published": iso_date,
            "source": source,
            "feed": feed_name,
            "category": article_category,
        })

    return articles


def fetch_feed(feed_name: str, feed_config: dict) -> list[dict]:
    """
    Fetch and parse a single RSS feed.
    Uses multi-tier HTTP strategy (requests → cloudscraper → curl).
    Returns a list of normalized article dicts.
    """
    url = feed_config["url"]

    try:
        xml_text = fetch_url(url, feed_name)

        if xml_text is None:
            method_hint = ""
            if not HAS_CURL_CFFI:
                method_hint = " (try: pip install curl_cffi)"
            logger.warning(f"[{feed_name}] All fetch methods failed — skipping.{method_hint}")
            return []

        articles = parse_feed_entries(xml_text, feed_name, feed_config)
        logger.info(f"[{feed_name}] Fetched {len(articles)} articles")
        return articles

    except Exception as e:
        logger.error(f"[{feed_name}] Unexpected error: {e}")
        return []

# ---------------------------------------------------------------------------
# Deduplication & sorting
# ---------------------------------------------------------------------------

def deduplicate(articles: list[dict]) -> list[dict]:
    """
    Remove duplicate articles (same article appearing in multiple S&P feeds).
    Keeps the first occurrence, but merges categories.
    """
    seen = {}
    for article in articles:
        aid = article["id"]
        if aid in seen:
            # Merge categories (e.g., article in both "Oil" and "Oil - Crude")
            existing_cats = seen[aid]["category"]
            new_cat = article["category"]
            if new_cat not in existing_cats:
                seen[aid]["category"] = f"{existing_cats}, {new_cat}"
        else:
            seen[aid] = article

    return list(seen.values())


def sort_by_date(articles: list[dict], descending: bool = True) -> list[dict]:
    """Sort articles by published date (newest first by default)."""
    def sort_key(a):
        d = a.get("published")
        if d is None:
            return ""
        return d

    return sorted(articles, key=sort_key, reverse=descending)

# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def load_existing_feed() -> dict:
    """Load the existing feed.json if it exists."""
    if OUTPUT_FILE.exists():
        try:
            with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            pass
    return {"articles": [], "metadata": {}}


def save_feed(
    articles: list[dict],
    fetch_stats: dict,
    sentiment_stats: Optional[dict[str, Any]] = None,
    ner_stats: Optional[dict[str, Any]] = None,
):
    """Save the unified feed to JSON."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    metadata = {
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "total_articles": len(articles),
        "feeds_fetched": fetch_stats.get("success", 0),
        "feeds_failed": fetch_stats.get("failed", 0),
        "feed_details": fetch_stats.get("details", {}),
    }
    if sentiment_stats:
        metadata["sentiment"] = sentiment_stats
    if ner_stats:
        metadata["ner"] = ner_stats

    output = {
        "metadata": metadata,
        "articles": articles,
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    logger.info(
        f"Saved {len(articles)} articles to {OUTPUT_FILE} "
        f"({fetch_stats.get('success', 0)} feeds OK, "
        f"{fetch_stats.get('failed', 0)} failed)"
    )

# ---------------------------------------------------------------------------
# Incremental merge
# ---------------------------------------------------------------------------

def merge_with_existing(new_articles: list[dict], max_age_days: int = 7) -> list[dict]:
    """
    Merge new articles with the existing feed.json.
    - Adds new articles that aren't already present
    - Drops articles older than max_age_days
    - Keeps the feed size manageable
    """
    existing = load_existing_feed()
    existing_articles = existing.get("articles", [])

    # Build lookup of existing IDs
    existing_by_id = {a["id"]: a for a in existing_articles}

    # Add genuinely new articles
    added = 0
    updated = 0
    for article in new_articles:
        existing_article = existing_by_id.get(article["id"])
        if existing_article is None:
            existing_articles.append(article)
            existing_by_id[article["id"]] = article
            added += 1
            continue

        # Refresh existing article fields from the latest scrape but keep any
        # existing enrichments until incremental scoring decides if a rescore is needed.
        prior_sentiment = existing_article.get("sentiment")
        prior_ner = existing_article.get("ner")
        existing_article.update(article)
        if prior_sentiment is not None:
            existing_article["sentiment"] = prior_sentiment
        if prior_ner is not None:
            existing_article["ner"] = prior_ner
        updated += 1

    # Filter out articles older than max_age_days
    cutoff = datetime.now(timezone.utc).isoformat()
    # (For simplicity, we keep all articles — the frontend can filter by date.)
    # If you want age-based pruning, uncomment and adjust:
    #
    # from datetime import timedelta
    # cutoff_dt = datetime.now(timezone.utc) - timedelta(days=max_age_days)
    # cutoff_iso = cutoff_dt.isoformat()
    # existing_articles = [
    #     a for a in existing_articles
    #     if (a.get("published") or "") >= cutoff_iso
    # ]

    logger.info(
        f"Merged: {added} new articles added, {updated} updated, "
        f"{len(existing_articles)} total"
    )

    return sort_by_date(existing_articles)

# ---------------------------------------------------------------------------
# Main scrape loop
# ---------------------------------------------------------------------------

def scrape_all() -> tuple[list[dict], dict]:
    """Fetch all feeds, normalize, deduplicate, and return articles + stats."""
    all_articles = []
    stats = {"success": 0, "failed": 0, "details": {}}

    for feed_name, feed_config in FEEDS.items():
        articles = fetch_feed(feed_name, feed_config)
        if articles:
            all_articles.extend(articles)
            stats["success"] += 1
            stats["details"][feed_name] = {
                "status": "ok",
                "count": len(articles),
            }
        else:
            stats["failed"] += 1
            stats["details"][feed_name] = {"status": "failed", "count": 0}

    # Deduplicate (articles may appear in multiple S&P category feeds)
    all_articles = deduplicate(all_articles)

    # Sort newest first
    all_articles = sort_by_date(all_articles)

    logger.info(
        f"Scrape complete: {len(all_articles)} unique articles "
        f"from {stats['success']}/{len(FEEDS)} feeds"
    )

    return all_articles, stats


def run_once(
    sentiment_config: Optional[SentimentConfig] = None,
    scorer: Optional[FinBERTScorer] = None,
    ner_config: Optional[NERConfig] = None,
    ner_extractor: Optional[SpacyNERExtractor] = None,
):
    """Single scrape run: fetch, merge with existing, (optional) enrichments, save."""
    new_articles, stats = scrape_all()
    merged = merge_with_existing(new_articles)
    sentiment_stats = None
    ner_stats = None

    if sentiment_config and sentiment_config.enabled:
        try:
            scorer = scorer or FinBERTScorer(sentiment_config)
            sentiment_stats = scorer.score_incremental(merged)
            logger.info(
                "Sentiment: scored %s, reused %s (candidates=%s, %sms)",
                sentiment_stats.get("scored", 0),
                sentiment_stats.get("reused", 0),
                sentiment_stats.get("candidate_articles", 0),
                sentiment_stats.get("duration_ms", 0),
            )
            if sentiment_stats.get("scored", 0) > 0:
                log_sentiment_rollup(merged)
        except Exception as e:
            logger.error(f"Sentiment scoring failed: {e}")
            sentiment_stats = {
                "enabled": True,
                "model": sentiment_config.model_name,
                "input_mode": (
                    "title+description" if sentiment_config.use_description else "title"
                ),
                "error": str(e),
            }

    if ner_config and ner_config.enabled:
        try:
            ner_extractor = ner_extractor or SpacyNERExtractor(ner_config)
            ner_stats = ner_extractor.extract_incremental(merged)
            logger.info(
                "NER: extracted %s, reused %s (candidates=%s, %sms)",
                ner_stats.get("extracted", 0),
                ner_stats.get("reused", 0),
                ner_stats.get("candidate_articles", 0),
                ner_stats.get("duration_ms", 0),
            )
            if ner_stats.get("extracted", 0) > 0:
                log_ner_rollup(merged)
        except Exception as e:
            logger.error(f"NER extraction failed: {e}")
            ner_stats = {
                "enabled": True,
                "model": ner_config.model_name,
                "input_mode": (
                    "title+description" if ner_config.use_description else "title"
                ),
                "error": str(e),
            }

    save_feed(merged, stats, sentiment_stats=sentiment_stats, ner_stats=ner_stats)
    return merged, stats, sentiment_stats, ner_stats


def run_daemon(
    interval: int = 600,
    sentiment_config: Optional[SentimentConfig] = None,
    ner_config: Optional[NERConfig] = None,
):
    """Continuous scrape loop."""
    logger.info(f"Starting daemon mode — polling every {interval}s")
    scorer = None
    if sentiment_config and sentiment_config.enabled:
        scorer = FinBERTScorer(sentiment_config)
    ner_extractor = None
    if ner_config and ner_config.enabled:
        ner_extractor = SpacyNERExtractor(ner_config)

    while True:
        try:
            run_once(
                sentiment_config=sentiment_config,
                scorer=scorer,
                ner_config=ner_config,
                ner_extractor=ner_extractor,
            )
        except KeyboardInterrupt:
            logger.info("Daemon stopped by user")
            break
        except Exception as e:
            logger.error(f"Scrape cycle failed: {e}")

        logger.info(f"Next poll in {interval}s...")
        time.sleep(interval)

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# HTTP server (for --serve mode)
# ---------------------------------------------------------------------------

class SilentHTTPHandler(http.server.SimpleHTTPRequestHandler):
    """SimpleHTTPRequestHandler with suppressed access logs."""

    def log_message(self, format, *args):
        pass  # suppress per-request stdout noise


def start_http_server(port: int, directory: str):
    """Serve the project directory over HTTP. Runs in a background thread."""
    os.chdir(directory)
    handler = SilentHTTPHandler
    with socketserver.TCPServer(("", port), handler) as httpd:
        httpd.allow_reuse_address = True
        logger.info(f"HTTP server running → http://localhost:{port}/")
        httpd.serve_forever()


def main():
    parser = argparse.ArgumentParser(description="Commodity News Feed RSS Scraper")
    parser.add_argument(
        "--daemon", action="store_true", help="Run continuously, polling on an interval"
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=600,
        help="Poll interval in seconds (default: 600 = 10 min)",
    )
    parser.add_argument(
        "--serve",
        action="store_true",
        help="Also start an HTTP server so you can open index.html in a browser",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="HTTP server port (default: 8000)",
    )
    parser.add_argument(
        "--sentiment",
        action="store_true",
        help="Run FinBERT sentiment analysis and store scores in feed.json",
    )
    parser.add_argument(
        "--sentiment-model",
        default="ProsusAI/finbert",
        help="HuggingFace model to use for sentiment (default: ProsusAI/finbert)",
    )
    parser.add_argument(
        "--sentiment-batch-size",
        type=int,
        default=32,
        help="Sentiment inference batch size (default: 32)",
    )
    parser.add_argument(
        "--sentiment-max-length",
        type=int,
        default=128,
        help="Sentiment max token length (default: 128)",
    )
    parser.add_argument(
        "--sentiment-use-description",
        action="store_true",
        help="Score title + description instead of title only",
    )
    parser.add_argument(
        "--sentiment-force-rescore",
        action="store_true",
        help="Rescore even if sentiment already exists for an article",
    )
    parser.add_argument(
        "--ner",
        action="store_true",
        help="Run spaCy entity + country extraction and store it in feed.json",
    )
    parser.add_argument(
        "--ner-model",
        default="en_core_web_lg",
        help="spaCy model to use for NER (default: en_core_web_lg)",
    )
    parser.add_argument(
        "--ner-batch-size",
        type=int,
        default=64,
        help="NER inference batch size (default: 64)",
    )
    parser.add_argument(
        "--ner-use-description",
        action="store_true",
        help="Run NER on title + description instead of title only",
    )
    parser.add_argument(
        "--ner-force-rescore",
        action="store_true",
        help="Re-extract NER even if cached results already exist",
    )
    parser.add_argument(
        "--ner-max-entities",
        type=int,
        default=18,
        help="Maximum stored entities per article for NER (default: 18)",
    )
    args = parser.parse_args()

    sentiment_config = SentimentConfig(
        enabled=args.sentiment,
        model_name=args.sentiment_model,
        batch_size=args.sentiment_batch_size,
        max_length=args.sentiment_max_length,
        use_description=args.sentiment_use_description,
        force_rescore=args.sentiment_force_rescore,
    )
    ner_config = NERConfig(
        enabled=args.ner,
        model_name=args.ner_model,
        batch_size=args.ner_batch_size,
        use_description=args.ner_use_description,
        force_rescore=args.ner_force_rescore,
        max_entities=args.ner_max_entities,
    )

    project_dir = str(Path(__file__).parent.resolve())

    if args.serve:
        # Launch HTTP server in a daemon thread so it dies when the main process exits
        server_thread = threading.Thread(
            target=start_http_server,
            args=(args.port, project_dir),
            daemon=True,
        )
        server_thread.start()
        logger.info(f"Open in browser: http://localhost:{args.port}/")

    if args.daemon or args.serve:
        # --serve alone implies daemon mode (keep refreshing feed.json)
        run_daemon(
            args.interval,
            sentiment_config=sentiment_config,
            ner_config=ner_config,
        )
    else:
        articles, stats, sentiment_stats, ner_stats = run_once(
            sentiment_config=sentiment_config,
            ner_config=ner_config,
        )
        print(f"\nDone! {len(articles)} articles saved to {OUTPUT_FILE}")
        print(f"Feeds OK: {stats['success']} | Failed: {stats['failed']}")
        if sentiment_stats and sentiment_stats.get("enabled"):
            if sentiment_stats.get("error"):
                print(f"Sentiment: failed ({sentiment_stats['error']})")
            else:
                print(
                    "Sentiment: "
                    f"scored={sentiment_stats.get('scored', 0)} | "
                    f"reused={sentiment_stats.get('reused', 0)} | "
                    f"candidates={sentiment_stats.get('candidate_articles', 0)}"
                )
        if ner_stats and ner_stats.get("enabled"):
            if ner_stats.get("error"):
                print(f"NER: failed ({ner_stats['error']})")
            else:
                print(
                    "NER: "
                    f"extracted={ner_stats.get('extracted', 0)} | "
                    f"reused={ner_stats.get('reused', 0)} | "
                    f"candidates={ner_stats.get('candidate_articles', 0)}"
                )

        # Print summary of what we got
        print("\nFeed breakdown:")
        for name, detail in stats["details"].items():
            status = "✓" if detail["status"] == "ok" else "✗"
            print(f"  {status} {name}: {detail['count']} articles")

        if args.serve:
            logger.info("Server running. Press Ctrl+C to stop.")
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                logger.info("Stopped.")


if __name__ == "__main__":
    main()
