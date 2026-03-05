"""
Contango — Commodity Article Classifier
========================================
Classifies commodity news articles into frontend filter categories using
keyword matching against commodity_taxonomy.json, plus a curated supplement
list for common terms not directly derivable from taxonomy commodity names.

Usage as a module (called by rss_scraper.py at scrape time):
    from classifier import classify_category
    category = classify_category(title, description)   # returns str or None

Usage as a standalone script (re-classify all ICIS articles in feed.json):
    python classifier.py                  # re-classify ICIS articles in data/feed.json
    python classifier.py --all            # re-classify every article regardless of source
    python classifier.py --input path/to/feed.json --output path/to/feed.json
    python classifier.py --dry-run        # print changes without writing

How it works:
    1. Loads commodity_taxonomy.json (categories → subcategories → commodities).
    2. Builds a list of (compiled_regex, filter_category) tuples from every
       commodity name, alias, and variant in the taxonomy.
    3. Appends supplement keywords for common shorthand, plural forms, and terms
       not represented in the taxonomy (e.g. "crude", "steel", "tanker").
    4. Sorts the list longest-keyword-first so specific phrases win over shorter
       ones (e.g. "liquefied natural gas" matches before "gas").
    5. Scans the article title + description for the first two distinct category
       matches and returns them joined by ", " (e.g. "Oil - Crude, Shipping").
    6. Returns None if no keywords matched — callers keep the article's original
       category (typically "General" for unclassified ICIS articles).

Extending the classifier:
    - To cover new filter pills: add an entry to _TAXONOMY_FILTER_MAP.
    - To fix a missed or wrong classification: add a line to _SUPPLEMENT_KEYWORDS.
      Supplement keywords are processed after taxonomy entries, so they can
      override taxonomy defaults for ambiguous terms.
    - To add an entirely new source or classification strategy: subclass or
      extend the KeywordClassifier class rather than editing the module globals.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
from pathlib import Path
from typing import Optional

logger = logging.getLogger("classifier")

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

TAXONOMY_PATH = Path(__file__).parent / "commodity_taxonomy.json"
DEFAULT_FEED_PATH = Path(__file__).parent / "data" / "feed.json"

# ---------------------------------------------------------------------------
# Taxonomy → frontend filter mapping
# ---------------------------------------------------------------------------
# Maps (taxonomy_category, taxonomy_subcategory) → frontend filter-pill value.
# Use None as the subcategory to match every subcategory under that category.
#
# To add a new filter pill (e.g. "Construction Materials"):
#   1. Add the pill to index.html
#   2. Add the (category, subcategory) → "PillLabel" entries here

_TAXONOMY_FILTER_MAP: dict[tuple[str, Optional[str]], str] = {
    ("Agriculture", "Fertilizers"):           "Fertilizers",
    ("Agriculture", "Grains and Oilseeds"):   "Agriculture",
    ("Agriculture", "Proteins and Feed"):     "Agriculture",
    ("Agriculture", "Sugar"):                 "Agriculture",
    ("Chemicals",   None):                    "Chemicals",
    # Construction Materials has no filter pill yet — add mapping when pill is added
    ("Energy",      "Biofuels"):              "Energy Transition",
    ("Energy",      "Coal and Coke"):         "Coal",
    ("Energy",      "Crude Oil"):             "Oil - Crude",
    ("Energy",      "Hydrogen and Ammonia"):  "Energy Transition",
    ("Energy",      "Natural Gas and LNG"):   "Natural Gas",   # LNG split below
    ("Energy",      "Power"):                 "Electric Power",
    ("Energy",      "Refined Products"):      "Oil - Refined Products",
    ("Environmental Markets", None):          "Energy Transition",
    ("Metals",      None):                    "Metals",
}

# Commodity names/aliases (lowercase) within the "Natural Gas and LNG"
# subcategory whose primary classification should be "LNG" not "Natural Gas".
_LNG_NAMES: frozenset[str] = frozenset({
    "liquefied natural gas", "lng", "bio-lng", "japan korea marker", "jkm",
})

# ---------------------------------------------------------------------------
# Supplement keywords
# ---------------------------------------------------------------------------
# Common shorthand, plural forms, spelling variants, and terms (e.g. Shipping)
# that aren't directly derivable from taxonomy commodity names.
#
# Format: (keyword_lowercase, frontend_filter_value)
# Processed AFTER taxonomy entries — supplements can override taxonomy defaults
# for ambiguous terms (later entries are added with deduplication so first wins
# for taxonomy; supplements only fill gaps).
#
# Keep entries grouped by filter category for readability.

_SUPPLEMENT_KEYWORDS: list[tuple[str, str]] = [
    # ── Oil – Crude ──────────────────────────────────────────────────────────
    ("crude oil",              "Oil - Crude"),
    ("crude",                  "Oil - Crude"),
    ("petroleum",              "Oil - Crude"),
    ("oil barrel",             "Oil - Crude"),
    ("oil prices",             "Oil - Crude"),
    # ── Oil – Refined Products ───────────────────────────────────────────────
    ("refined products",       "Oil - Refined Products"),
    ("refinery",               "Oil - Refined Products"),
    ("refining",               "Oil - Refined Products"),
    ("refining margins",       "Oil - Refined Products"),
    # ── Natural Gas ──────────────────────────────────────────────────────────
    ("natural gas",            "Natural Gas"),
    ("pipeline gas",           "Natural Gas"),
    ("gas prices",             "Natural Gas"),
    # ── LNG ──────────────────────────────────────────────────────────────────
    ("lng",                    "LNG"),
    ("liquefied natural gas",  "LNG"),
    # ── Coal ─────────────────────────────────────────────────────────────────
    ("coal",                   "Coal"),
    ("coking coal",            "Coal"),
    ("thermal coal",           "Coal"),
    # ── Electric Power ───────────────────────────────────────────────────────
    ("electricity",            "Electric Power"),
    ("power prices",           "Electric Power"),
    ("power grid",             "Electric Power"),
    # ── Energy Transition ────────────────────────────────────────────────────
    ("renewables",             "Energy Transition"),
    ("renewable energy",       "Energy Transition"),
    ("energy transition",      "Energy Transition"),
    ("solar",                  "Energy Transition"),
    ("wind power",             "Energy Transition"),
    ("wind energy",            "Energy Transition"),
    ("wind farm",              "Energy Transition"),
    ("offshore wind",          "Energy Transition"),
    ("onshore wind",           "Energy Transition"),
    ("solar power",            "Energy Transition"),
    ("solar capacity",         "Energy Transition"),
    ("clean energy",           "Energy Transition"),
    ("green energy",           "Energy Transition"),
    ("carbon credits",         "Energy Transition"),
    ("carbon allowances",      "Energy Transition"),
    ("emissions allowances",   "Energy Transition"),
    ("emissions trading",      "Energy Transition"),
    ("carbon market",          "Energy Transition"),
    ("carbon price",           "Energy Transition"),
    ("decarbonisation",        "Energy Transition"),
    ("decarbonization",        "Energy Transition"),
    # ── Chemicals ────────────────────────────────────────────────────────────
    ("petrochemicals",         "Chemicals"),
    ("polymers",               "Chemicals"),
    ("plastics",               "Chemicals"),
    # ── Metals ───────────────────────────────────────────────────────────────
    ("iron ore",               "Metals"),
    ("scrap metal",            "Metals"),
    ("steel",                  "Metals"),
    ("aluminium",              "Metals"),
    ("aluminum",               "Metals"),
    ("copper",                 "Metals"),
    ("stainless steel",        "Metals"),
    # ── Agriculture ──────────────────────────────────────────────────────────
    ("grains",                 "Agriculture"),
    ("oilseeds",               "Agriculture"),
    ("vegetable oil",          "Agriculture"),
    ("palm oil",               "Agriculture"),
    # ── Fertilizers ──────────────────────────────────────────────────────────
    ("fertilizer",             "Fertilizers"),
    ("fertiliser",             "Fertilizers"),
    # ── Shipping (not in taxonomy; S&P has a dedicated feed — classify ICIS too)
    ("shipping rates",         "Shipping"),
    ("freight rates",          "Shipping"),
    ("tanker rates",           "Shipping"),
    ("tanker",                 "Shipping"),
    ("vlcc",                   "Shipping"),
    ("bunker fuel",            "Shipping"),
    ("charter rate",           "Shipping"),
]

# ---------------------------------------------------------------------------
# Keyword index — built once, cached for the process lifetime
# ---------------------------------------------------------------------------

# Each entry: (compiled_regex, filter_category, keyword_length)
_KeywordIndex = list[tuple[re.Pattern[str], str, int]]

_KEYWORD_INDEX: Optional[_KeywordIndex] = None


def build_keyword_index() -> _KeywordIndex:
    """
    Build and return the keyword classification index.

    Loads commodity_taxonomy.json, derives keywords from every commodity name,
    alias, and variant, then appends the supplement list.  Sorted longest-first
    so more specific phrases beat shorter ones.
    """
    entries: _KeywordIndex = []
    seen: set[str] = set()

    def _add(kw: str, cat: str) -> None:
        key = kw.strip().lower()
        if not key or len(key) < 2 or key in seen:
            return
        seen.add(key)
        # Negative lookbehind/ahead enforces word boundaries without relying
        # solely on \b, which handles hyphenated / numeric terms correctly.
        pattern = re.compile(
            r"(?<![A-Za-z0-9])" + re.escape(key) + r"(?![A-Za-z0-9])",
            re.IGNORECASE,
        )
        entries.append((pattern, cat, len(key)))

    # ── 1. Taxonomy-derived keywords ─────────────────────────────────────────
    if TAXONOMY_PATH.exists():
        try:
            with open(TAXONOMY_PATH, encoding="utf-8") as f:
                taxonomy = json.load(f)

            for cat in taxonomy.get("categories", []):
                cat_name: str = cat["name"]
                for sub in cat.get("subcategories", []):
                    sub_name: str = sub["name"]

                    # Resolve filter category for this subcategory
                    filter_cat = _TAXONOMY_FILTER_MAP.get((cat_name, sub_name))
                    if filter_cat is None:
                        filter_cat = _TAXONOMY_FILTER_MAP.get((cat_name, None))
                    if filter_cat is None:
                        continue  # e.g. Construction Materials — no pill yet

                    # Index the subcategory name itself
                    _add(sub_name, filter_cat)

                    for commodity in sub.get("commodities", []):
                        fc = filter_cat
                        # LNG disambiguation within "Natural Gas and LNG"
                        if cat_name == "Energy" and sub_name == "Natural Gas and LNG":
                            cname_lower = commodity["name"].lower()
                            aliases_lower = [a.lower() for a in commodity.get("aliases", [])]
                            if cname_lower in _LNG_NAMES or any(
                                a in _LNG_NAMES for a in aliases_lower
                            ):
                                fc = "LNG"

                        terms = (
                            [commodity["name"]]
                            + commodity.get("aliases", [])
                            + commodity.get("variants", [])
                        )
                        for term in terms:
                            _add(term, fc)

        except Exception as exc:
            logger.warning(f"Could not load taxonomy for classification: {exc}")
    else:
        logger.warning(
            f"Taxonomy not found at {TAXONOMY_PATH}; articles will remain unclassified."
        )

    # ── 2. Supplement keywords ────────────────────────────────────────────────
    for kw, cat in _SUPPLEMENT_KEYWORDS:
        _add(kw, cat)

    # Sort longest-first so more specific phrases win over shorter ones
    entries.sort(key=lambda x: -x[2])

    logger.info(f"Keyword index built: {len(entries)} keywords")
    return entries


def _get_keyword_index() -> _KeywordIndex:
    """Return the cached keyword index, building it on first call."""
    global _KEYWORD_INDEX
    if _KEYWORD_INDEX is None:
        _KEYWORD_INDEX = build_keyword_index()
    return _KEYWORD_INDEX


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def classify_category(title: str, description: str = "") -> Optional[str]:
    """
    Classify a commodity article into up to two frontend filter categories.

    Keyword-matches the article title and description against the taxonomy
    index and returns up to two distinct category matches joined by ", "
    (e.g. "Oil - Crude" or "Oil - Crude, Shipping").

    Returns None if no commodity keywords matched, leaving the caller's
    original category unchanged.

    Args:
        title:       Article headline text.
        description: Optional article summary / description text.

    Returns:
        A category string, a comma-joined pair of categories, or None.
    """
    text = f"{title} {description}"
    index = _get_keyword_index()
    matched: list[str] = []
    seen_cats: set[str] = set()

    for pattern, category, _ in index:
        if category in seen_cats:
            continue
        if pattern.search(text):
            seen_cats.add(category)
            matched.append(category)
            if len(matched) >= 2:
                break

    return ", ".join(matched) if matched else None


# ---------------------------------------------------------------------------
# Standalone script — re-classify articles in feed.json
# ---------------------------------------------------------------------------

def _reclassify_feed(
    input_path: Path,
    output_path: Path,
    all_sources: bool = False,
    dry_run: bool = False,
) -> None:
    """
    Load feed.json, re-classify articles, and write back.

    By default only ICIS articles (source == "ICIS") are reclassified, since
    S&P feeds already carry the correct category from the scraper.  Pass
    all_sources=True to reclassify every article regardless of source.
    """
    if not input_path.exists():
        logger.error(f"Feed file not found: {input_path}")
        return

    with open(input_path, encoding="utf-8") as f:
        feed = json.load(f)

    articles: list[dict] = feed.get("articles", [])
    changed = 0
    unchanged = 0
    skipped = 0

    for article in articles:
        source: str = article.get("source", "")
        title: str = article.get("title", "")
        description: str = article.get("description", "")
        old_cat: str = article.get("category", "General")

        # Decide whether to reclassify this article
        if not all_sources and source != "ICIS":
            skipped += 1
            continue

        classified = classify_category(title, description)
        new_cat = classified if classified else "General"

        if new_cat != old_cat:
            if not dry_run:
                article["category"] = new_cat
            changed += 1
            logger.debug(f'  "{title[:60]}" → {old_cat!r} ⟶ {new_cat!r}')
        else:
            unchanged += 1

    total_processed = changed + unchanged
    print(
        f"{'[DRY RUN] ' if dry_run else ''}"
        f"Processed {total_processed} articles "
        f"({'all sources' if all_sources else 'ICIS only'}): "
        f"{changed} reclassified, {unchanged} unchanged, {skipped} skipped"
    )

    if not dry_run:
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(feed, f, indent=2, ensure_ascii=False)
        print(f"Saved → {output_path}")


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    parser = argparse.ArgumentParser(
        description="Re-classify commodity articles in feed.json using the taxonomy keyword index."
    )
    parser.add_argument(
        "--input",
        default=str(DEFAULT_FEED_PATH),
        help=f"Path to feed.json (default: {DEFAULT_FEED_PATH})",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output path (default: same as --input, overwrites in place)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        dest="all_sources",
        help="Re-classify all articles regardless of source (default: ICIS only)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would change without writing to disk",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Log each reclassification at DEBUG level",
    )
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    input_path = Path(args.input)
    output_path = Path(args.output) if args.output else input_path

    _reclassify_feed(
        input_path=input_path,
        output_path=output_path,
        all_sources=args.all_sources,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
