import json
import time
import random
import requests
from pathlib import Path
from bs4 import BeautifulSoup

# ─── CONFIG ───────────────────────────────────────────────────────────────────
FILTERED_JSON        = Path("../filtered_articles.json")
IMAGE_INDEX_JSON     = Path("downloaded_images_index.json")
OUTPUT_JSON          = Path("downloaded_images_index_with_fox_captions_full.json")
USER_AGENT           = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36"
)
REQUEST_DELAY_RANGE  = (2.0, 4.0)    # seconds between page fetches

# ─── LOAD DATA ────────────────────────────────────────────────────────────────
print("Loading filtered articles and image index…")
with FILTERED_JSON.open(encoding="utf-8") as f:
    articles = json.load(f)
id2url = { art["id"]: art["url"] for art in articles }

with IMAGE_INDEX_JSON.open(encoding="utf-8") as f:
    records = json.load(f)

# select ALL Fox News records
fox_records = [rec for rec in records if rec["source_domain"] == "foxnews.com"]
print(f"→ Will update captions for {len(fox_records)} Fox News images")

# ─── SCRAPER SETUP ─────────────────────────────────────────────────────────────
session = requests.Session()
session.headers.update({"User-Agent": USER_AGENT})

def scrape_captions_for_url(page_url):
    """
    Fetches a Fox News article and returns a dict:
      { base_image_url_without_query: real_caption_text, … }
    """
    resp = session.get(page_url, timeout=10)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    cmap = {}
    for block in soup.select("div.image-ct.inline"):
        img     = block.find("img", src=True)
        cap_div = block.select_one("div.info div.caption")
        if not img or not cap_div:
            continue
        base = img["src"].split("?", 1)[0]
        caption = " ".join(
            span.get_text(strip=True)
            for span in cap_div.find_all("span")
        )
        cmap[base] = caption
    return cmap

# ─── UPDATE LOOP (SEQUENTIAL) ─────────────────────────────────────────────────
updated = []
last_url    = None
caption_map = {}

for idx, rec in enumerate(fox_records, start=1):
    art_id   = rec["article_id"]
    page_url = id2url.get(art_id)
    print(f"\n[{idx}/{len(fox_records)}] image_id={rec['image_id']} (article {art_id})")

    if not page_url:
        print(f"  ⚠️  No URL for article {art_id}, skipping fetch")
        updated.append(rec)
        continue

    # fetch and parse captions on the first occurrence of each article
    if page_url != last_url:
        print(f"  → Fetching {page_url}")
        try:
            caption_map = scrape_captions_for_url(page_url)
            last_url    = page_url
            print(f"    ✔ Scraped {len(caption_map)} captions")
        except Exception as e:
            print(f"    ❌ Error scraping {page_url}: {e}")
        delay = random.uniform(*REQUEST_DELAY_RANGE)
        print(f"    ⏱ Sleeping for {delay:.1f}s")
        time.sleep(delay)

    # join on base image URL to update caption
    base = rec["image_url"].split("?", 1)[0]
    old_caption = rec.get("caption") or ""
    new_caption = caption_map.get(base, old_caption) or ""

    rec["caption"] = new_caption
    mark = "✔" if new_caption != old_caption else "—"
    snippet = (new_caption[:60] + "…") if len(new_caption) > 60 else new_caption
    print(f"    {mark} Updated caption: {snippet!r}")

    updated.append(rec)

# ─── WRITE FULL OUTPUT ────────────────────────────────────────────────────────
with OUTPUT_JSON.open("w", encoding="utf-8") as f:
    json.dump(updated, f, ensure_ascii=False, indent=2)

print(f"\n✅ All updated captions written to {OUTPUT_JSON}")
