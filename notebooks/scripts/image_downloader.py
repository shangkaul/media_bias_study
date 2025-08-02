import os
import json
import logging
import time
import random
import uuid
from urllib.parse import urlparse, unquote
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ─── CONFIGURE LOGGING ────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("download_images.log")
    ],
    force=True
)
logger = logging.getLogger()

# ─── HTTP SESSION WITH RETRIES ───────────────────────────────────────────────
session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8"
})
retry = Retry(
    total=5,
    backoff_factor=1,
    status_forcelist=[500, 502, 503, 504],
    allowed_methods=["HEAD","GET","OPTIONS"]
)
adapter = HTTPAdapter(max_retries=retry)
session.mount("http://", adapter)
session.mount("https://", adapter)

ALLOWED_SUFFIXES = {".jpg", ".jpeg", ".png", ".webp"}

def sanitize_filename(url: str) -> str:
    return unquote(Path(urlparse(url).path).name)

def extract_url_caption(entry):
    """
    Return (image_url, caption, other_fields_dict)
    """
    if isinstance(entry, str):
        return entry, None, {}
    for k in ("image_url","url","src"):
        if k in entry:
            url = entry[k]
            break
    else:
        return None, None, {}
    caption = entry.get("caption") or entry.get("credit")
    other   = {kk: entry[kk] for kk in entry if kk not in { "image_url","url","src","caption","credit" }}
    return url, caption, other

def download_image(url, save_dir: Path, prefix: str, referer: str):
    fname = f"{prefix}_{sanitize_filename(url)}"
    out = save_dir / fname
    ext = out.suffix.lower()
    if ext not in ALLOWED_SUFFIXES:
        out = out.with_suffix(ext + ".jpg")
    if out.exists():
        return str(out)

    headers = {"Referer": referer}
    for attempt in range(2):
        try:
            logger.info("↓ %s", url)
            resp = session.get(url, stream=True, timeout=10, headers=headers)
            if resp.status_code == 429 and attempt == 0:
                logger.warning("429 rate-limit on %s, backing off 10s…", url)
                time.sleep(10)
                continue
            resp.raise_for_status()

            save_dir.mkdir(parents=True, exist_ok=True)
            tmp = out.with_suffix(".tmp")
            with open(tmp, "wb") as f:
                for chunk in resp.iter_content(8192):
                    f.write(chunk)
            tmp.rename(out)
            logger.info("✔ %s", out)
            return str(out)

        except Exception as e:
            logger.warning("  ✖ %s → %s: %s", url, out, e)
            break

    if out.exists():
        out.unlink()
    return None

# ─── PARAMETERS ──────────────────────────────────────────────────────────────
INPUT_JSON           = "../filtered_articles.json"
OUTPUT_JSON          = "downloaded_images_index.json"
IMG_ROOT             = Path("img")
DRY_LIMIT_PER_SOURCE = None

# ─── LOAD ALL ARTICLES ─────────────────────────────────────────────────────────
with open(INPUT_JSON, encoding="utf-8") as f:
    articles = json.load(f)

# ─── GROUP BY SOURCE ──────────────────────────────────────────────────────────
by_source = {}
for art in articles:
    src = art.get("source_domain", "UNKNOWN")
    by_source.setdefault(src, []).append(art)

logger.info("Found %d sources: %s", len(by_source), list(by_source.keys()))

downloaded_records = []

def process_source(src, src_articles):
    logger.info("→ [%s] Starting (%d articles)", src, len(src_articles))
    limit = DRY_LIMIT_PER_SOURCE or len(src_articles)
    processed = 0
    recs = []

    for art in src_articles:
        if processed >= limit:
            break
        page_url = art.get("url","")
        p = urlparse(page_url)
        referer = f"{p.scheme}://{p.netloc}{p.path}"
        dest = IMG_ROOT / src / art["id"]

        for idx, entry in enumerate(art.get("images", []), start=1):
            img_url, cap, other = extract_url_caption(entry)
            if not img_url:
                continue

            prefix = f"{src}_{processed+1}_{idx}"
            local = download_image(img_url, dest, prefix, referer)
            time.sleep(random.uniform(0.1,0.3))

            # build a flat record with a random image_id
            rec = {
                "image_id":       str(uuid.uuid4()),
                "article_id":     art["id"],
                "source_domain":  src,
                "image_url":      img_url,
                "caption":        cap,
                "local_img_path": local
            }
            rec.update(other)
            recs.append(rec)

        processed += 1

    logger.info("✔ [%s] Done, processed %d articles", src, processed)
    return src, recs

# ─── PARALLELIZE ACROSS SOURCES ───────────────────────────────────────────────
workers = min(17, len(by_source))
with ThreadPoolExecutor(max_workers=workers) as ex:
    futures = {ex.submit(process_source, src, arts): src
               for src, arts in by_source.items()}
    for fut in as_completed(futures):
        src = futures[fut]
        try:
            _, recs = fut.result()
            downloaded_records.extend(recs)
            with open(f"{OUTPUT_JSON}.{src}.json", "w", encoding="utf-8") as ck:
                json.dump(recs, ck, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error("✖ [%s] Failed: %s", src, e)

# ─── WRITE FINAL INDEX JSON ──────────────────────────────────────────────────
logger.info("Writing final index (%d records) to %s", len(downloaded_records), OUTPUT_JSON)
with open(OUTPUT_JSON, "w", encoding="utf-8") as out:
    json.dump(downloaded_records, out, indent=2, ensure_ascii=False)

logger.info("All done.")
