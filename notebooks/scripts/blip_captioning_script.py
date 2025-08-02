import os
import signal
import tempfile
import logging
import pandas as pd
import math
import torch
from datetime import datetime
from PIL import Image
from torch.utils.data import Dataset, DataLoader
from transformers import BlipProcessor, BlipForConditionalGeneration
import pyarrow.parquet as pq
from tqdm.auto import tqdm

# ─── Configuration ─────────────────────────────────────────────────────────────
INPUT_PARQUET    = "data/image_index.parquet"
OUTPUT_PARQUET   = "data/blip_captioned_images.parquet"
TEMP_PARQUET     = OUTPUT_PARQUET + ".part"
TMP_DIR          = "data/tmp"
BATCH_SIZE       = 16
CHECKPOINT_EVERY = 50  # save progress every N batches

# Ensure tmp dir exists
os.makedirs(TMP_DIR, exist_ok=True)

# ─── Logging Setup ─────────────────────────────────────────────────────────────
logging.basicConfig(
    filename="blip_captioning.log",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)
logger.info("===== Script started =====")

# ─── Graceful Shutdown Handling ────────────────────────────────────────────────
shutdown_requested = False
def _on_shutdown(signum, frame):
    global shutdown_requested
    logger.warning("Received signal %s; will checkpoint and exit.", signum)
    shutdown_requested = True

signal.signal(signal.SIGINT,  _on_shutdown)
signal.signal(signal.SIGTERM, _on_shutdown)

try:
    # ─── Load or Resume Progress ─────────────────────────────────────────────
    if os.path.exists(TEMP_PARQUET):
        logger.info("Resuming from checkpoint: %s", TEMP_PARQUET)
        df_all = pd.read_parquet(TEMP_PARQUET)
    else:
        df_all = pq.read_table(INPUT_PARQUET).to_pandas()
        df_all["blip_caption"] = ""
        logger.info("Loaded %d image paths from %s", len(df_all), INPUT_PARQUET)

    paths_all = df_all["local_img_path"].tolist()
    n_total   = len(paths_all)
    logger.info("Total images in index: %d", n_total)

    # ─── Build list of indices to process ────────────────────────────────────
    valid_indices = []
    for i, p in enumerate(paths_all):
        if (
            isinstance(p, str) and p
            and os.path.isfile(p)
            and not df_all.at[i, "blip_caption"]
        ):
            valid_indices.append(i)
    n_valid = len(valid_indices)
    logger.info("Images to (re)process: %d", n_valid)

    # ─── Dataset & Collate ───────────────────────────────────────────────────
    class SafeImageDataset(Dataset):
        def __init__(self, indices, paths):
            self.indices = indices
            self.paths    = paths
        def __len__(self):
            return len(self.indices)
        def __getitem__(self, batch_idx):
            idx  = self.indices[batch_idx]
            path = self.paths[idx]
            try:
                img = Image.open(path).convert("RGB")
            except Exception as e:
                logger.error("Failed loading %s: %s", path, e)
                return None, idx
            return img, idx

    def collate_fn(batch):
        images, idxs = [], []
        for img, idx in batch:
            if img is None:
                df_all.at[idx, "blip_caption"] = ""
            else:
                images.append(img)
                idxs.append(idx)
        return images, idxs

    loader = DataLoader(
        SafeImageDataset(valid_indices, paths_all),
        batch_size=BATCH_SIZE,
        shuffle=False,
        num_workers=4,
        pin_memory=True,
        prefetch_factor=2,
        collate_fn=collate_fn
    )

    # ─── Model init ───────────────────────────────────────────────────────────
    device    = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    processor = BlipProcessor.from_pretrained("Salesforce/blip-image-captioning-base")
    model     = (
        BlipForConditionalGeneration
        .from_pretrained("Salesforce/blip-image-captioning-base")
        .to(device)
        .eval()
    )
    logger.info("Loaded BLIP model on %s", device)

    # ─── Caption loop with checkpointing & OOM handling ──────────────────────
    batch_count   = 0
    total_batches = math.ceil(n_valid / BATCH_SIZE)

    for images, idxs in tqdm(loader, total=total_batches, desc="Captioning"):
        batch_count += 1
        try:
            inputs = processor(images=images, return_tensors="pt", padding=True)
            inputs = {k: v.to(device, torch.float16) for k, v in inputs.items()}
            with torch.no_grad():
                outs = model.generate(**inputs, max_new_tokens=50)
            caps = processor.batch_decode(outs, skip_special_tokens=True)
            for i, cap in zip(idxs, caps):
                df_all.at[i, "blip_caption"] = cap

        except RuntimeError as e:
            if "out of memory" in str(e):
                logger.warning("OOM at batch %d; clearing cache and skipping", batch_count)
                torch.cuda.empty_cache()
            else:
                logger.exception("Error at batch %d", batch_count)

        # ─── Checkpointing ───────────────────────────────────────────────
        if batch_count % CHECKPOINT_EVERY == 0 or shutdown_requested:
            with tempfile.NamedTemporaryFile(
                delete=False,
                suffix=".parquet",
                dir=TMP_DIR
            ) as tmp:
                df_all.to_parquet(tmp.name, index=False)
                os.replace(tmp.name, TEMP_PARQUET)
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            logger.info("[%s] Checkpoint at batch %d/%d saved to %s",
                        now, batch_count, total_batches, TEMP_PARQUET)

        if shutdown_requested:
            logger.info("Shutdown requested; exiting after checkpoint.")
            break

    # ─── Final atomic save ─────────────────────────────────────────────────
    with tempfile.NamedTemporaryFile(
        delete=False,
        suffix=".parquet",
        dir=TMP_DIR
    ) as final_tmp:
        df_all.to_parquet(final_tmp.name, index=False)
        os.replace(final_tmp.name, OUTPUT_PARQUET)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info("[%s] Final output written to %s", now, OUTPUT_PARQUET)

except Exception:
    logger.exception("Script terminated with an exception")
    raise

logger.info("===== Script finished successfully =====")
