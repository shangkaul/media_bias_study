#!/usr/bin/env python3
import os
import json
import logging
import time
import random
from urllib.parse import urlparse, unquote
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed

# ─── CONFIGURE LOGGING ────────────────────────────────────────────────────────
LOG_FILE = "download_images.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ─── SETUP SESSION WITH RETRIES ───────────────────────────────────────────────
session = requests.Session()
retry_strategy = Retry(
    total=5,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["HEAD", "GET", "OPTIONS"]
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("https://", adapter)
session.mount("http://", adapter)
session.headers.update({
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/114.0.0.0 Safari/537.36"),
    "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
})

def read_articles(file_path):
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
        if not isinstance(data, list):
            logger.warning("Expected a list in %s", file_path)
            return []
        return data
    except FileNotFoundError:
        logger.error("File not found: %s", file_path)
    except json.JSONDecodeError:
        logger.error("Could not decode JSON from %s", file_path)
    return []

def sanitize_filename(url: str) -> str:
    parsed = urlparse(url)
    return unquote(os.path.basename(parsed.path))

def download_image(image_url, save_dir, prefix, timeout=10):
    filename = f"{prefix}_{sanitize_filename(image_url)}"
    save_path = os.path.join(save_dir, filename)
    if os.path.exists(save_path):
        logger.debug("Skipping existing %s", save_path)
        return save_path

    referer = f"{urlparse(image_url).scheme}://{urlparse(image_url).netloc}/"
    try:
        logger.info("Downloading %s → %s", image_url, save_path)
        resp = session.get(image_url,
                           stream=True,
                           timeout=timeout,
                           headers={"Referer": referer})
        resp.raise_for_status()
        os.makedirs(save_dir, exist_ok=True)
        with open(save_path, 'wb') as out:
            for chunk in resp.iter_content(8192):
                out.write(chunk)
        logger.info("Saved %s", save_path)
        return save_path

    except Exception as e:
        logger.error("Failed %s: %s", image_url, e)
        if os.path.exists(save_path):
            os.remove(save_path)
        return None

def process_source(src_name, json_path, image_dir, output_dir,
                   num_articles=None, start_idx=0):
    logger.info("=== %s: starting ===", src_name)
    articles = read_articles(json_path)
    if not articles:
        return

    end_idx = min(start_idx + num_articles, len(articles)) if num_articles else len(articles)
    for idx in range(start_idx, end_idx):
        logger.info("[%s] Article %d/%d", src_name, idx+1, len(articles))
        local_paths = []
        for img_i, url in enumerate(articles[idx].get('images', [])):
            prefix = f"{src_name}_art{idx+1}_img{img_i+1}"
            path = download_image(url, image_dir, prefix)
            if path:
                local_paths.append(path)
            time.sleep(random.uniform(0.1, 0.3))
        articles[idx]['local_images'] = local_paths

    os.makedirs(output_dir, exist_ok=True)
    out_path = os.path.join(output_dir, f"{src_name}_with_local_images.json")
    with open(out_path, 'w') as f:
        json.dump(articles, f, indent=2)
    logger.info("=== %s: done, wrote %s ===", src_name, out_path)

if __name__ == "__main__":
    data_path        = "../newscrawler/data"
    image_save_dir   = "../data/images"
    updated_json_dir = "../data/with_local_paths"
    NUM_ARTICLES     = None   # None = all articles; or e.g. 5

    news_files = {
        "cnn":    f"{data_path}/cnn_articles_20250601.json",
        "ht":     f"{data_path}/hindustan_times_articles_20250601.json",
        "ind":    f"{data_path}/india_articles_20250528.json",
        "nbc":    f"{data_path}/nbcnews_articles_20250528.json",
        "news18": f"{data_path}/news18_articles_20250601.json",
        "nweek":  f"{data_path}/newsweek_articles_20250601.json",
        "nypost": f"{data_path}/nypost_articles_20250601.json",
        "bbc":    f"{data_path}/bbcnews_articles_20250513.json",
    }

    # ─── PARALLELIZE ACROSS SOURCES ──────────────────────────────────────────
    with ThreadPoolExecutor(max_workers=len(news_files)) as exec:
        futures = {
            exec.submit(
                process_source,
                src,
                path,
                image_save_dir,
                updated_json_dir,
                NUM_ARTICLES,
                0
            ): src
            for src, path in news_files.items()
        }
        for fut in as_completed(futures):
            src = futures[fut]
            try:
                fut.result()
            except Exception as e:
                logger.error("Source %s raised: %s", src, e)
