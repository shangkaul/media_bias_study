import json
import time
import random
import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup

# ─── CONFIG ────────────────────────────────────────────────────────────────────
INPUT_JSON        = Path("../bbc_filtered.json")
OUTPUT_JSON       = Path("../bbc_captions_fixed.json")
MAX_WORKERS       = 5
TEST_ARTICLE_LIMIT = None     # ← only process first 10 articles for this test
REQUEST_DELAY_MIN = 1.0     # stagger between page‐fetches
REQUEST_DELAY_MAX = 2.0

# logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("bbc-caption-fixer")

# ─── HELPERS ───────────────────────────────────────────────────────────────────
def fetch_soup(url: str) -> BeautifulSoup:
    """Fetch a URL and return a BeautifulSoup, with simple retry/backoff on 429."""
    headers = {"User-Agent": "Mozilla/5.0"}
    for attempt in range(3):
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code == 429:
            backoff = 5 * (attempt + 1)
            logger.warning("429 on %s – sleeping %ds", url, backoff)
            time.sleep(backoff)
            continue
        resp.raise_for_status()
        return BeautifulSoup(resp.text, "html.parser")
    raise RuntimeError(f"Failed to fetch {url} after retries")

def extract_caption_for_img(img_tag):
    """Given an <img> tag, find its <figcaption> or .caption div."""
    if not img_tag:
        return ""
    # try <figure><figcaption>
    figure = img_tag.find_parent("figure")
    if figure:
        fc = figure.find("figcaption")
        if fc and fc.get_text(strip=True):
            return fc.get_text(" ", strip=True)
    # fallback: any sibling <div class="caption">
    cap_div = img_tag.find_next_sibling("div", class_="caption")
    if cap_div and cap_div.get_text(strip=True):
        return cap_div.get_text(" ", strip=True)
    return ""

def extract_title(soup: BeautifulSoup) -> str:
    """Grab the first <h1> from the page."""
    h1 = soup.find("h1")
    return h1.get_text(" ", strip=True) if h1 else ""

# ─── WORKER ────────────────────────────────────────────────────────────────────
def backfill_article(art: dict) -> dict:
    """Fetch the article page, fix title if missing, and then fix each image's caption."""
    url = art["url"]
    try:
        soup = fetch_soup(url)

        # ─ Fix missing title ─────────────────────────────────────────────
        if not art.get("title"):
            new_title = extract_title(soup)
            if new_title:
                art["title"] = new_title
                logger.info("✓ Fixed title for %s: %r", art.get("id") or url, new_title)
            else:
                logger.warning("⚠ No <h1> found to fix title for %s", art.get("id") or url)

        # ─ Fix captions ─────────────────────────────────────────────────
        for img_obj in art.get("images", []):
            src = img_obj.get("image_url")
            if not src:
                continue
            img_tag = soup.find("img", {"src": src})
            cap = extract_caption_for_img(img_tag)
            img_obj["caption"] = cap

        logger.info("✓ Captions fixed for article %s", art.get("id") or url)
    except Exception as e:
        logger.error("✖ Failed to backfill %s: %s", url, e)

    # throttle between articles
    time.sleep(random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX))
    return art

# ─── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    # load & slice for test
    all_articles = json.loads(INPUT_JSON.read_text(encoding="utf-8"))
    articles     = all_articles[:TEST_ARTICLE_LIMIT]
    total        = len(articles)
    logger.info("Loaded %d BBC articles to fix (test subset)", total)

    updated = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(backfill_article, art): idx for idx, art in enumerate(articles)}
        for fut in as_completed(futures):
            idx       = futures[fut]
            art_fixed = fut.result()
            updated.append((idx, art_fixed))

    # restore original order
    updated.sort(key=lambda x: x[0])
    fixed_articles = [art for _, art in updated]

    # write out
    OUTPUT_JSON.write_text(
        json.dumps(fixed_articles, indent=2, ensure_ascii=False),
        encoding="utf-8"
    )
    logger.info("All done — wrote captions‐fixed JSON to %s", OUTPUT_JSON)

if __name__ == "__main__":
    main()
