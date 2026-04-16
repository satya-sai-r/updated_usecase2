import asyncio
import json
import logging
import os
import shutil
from pathlib import Path

import duckdb
import nats
from dotenv import load_dotenv
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [ingestion] %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler("logs/ingestion.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

UPLOADS   = Path(os.getenv("UPLOADS_DIR", "uploads"))
PROCESSED = UPLOADS / "processed"
NATS_URL  = os.getenv("NATS_URL", "nats://localhost:4222")

REQUIRED_COLUMNS = [
    "distributor_id",
    "retailer_id",
    "sku_name",
    "product_category_snapshot",
    "secondary_transaction_id",
    "transaction_date",
    "secondary_gross_value",
    "secondary_tax_amount",
    "secondary_net_value",
]

def read_excel(path: str) -> list[dict]:
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    cols = ", ".join(REQUIRED_COLUMNS)
    query = f"SELECT {cols} FROM read_xlsx('{path}') WHERE distributor_id IS NOT NULL"
    
    rows = con.execute(query).fetchall()
    result = []
    for row in rows:
        d = dict(zip(REQUIRED_COLUMNS, row))
        if d.get("transaction_date"): d["transaction_date"] = str(d["transaction_date"])
        # Standardize ID to string to prevent decimal issues (.0)
        if d.get("secondary_transaction_id"):
            val = d["secondary_transaction_id"]
            if isinstance(val, float): val = str(int(val))
            d["secondary_transaction_id"] = str(val)
        result.append(d)
    return result

async def emit_events(path: str, nc) -> None:
    path_obj = Path(path)
    if "processed" in path_obj.parts: return # Skip processed folder

    log.info(f"Processing: {path_obj.name}")
    try:
        # Give a small delay for the file to be fully written by the OS/User
        await asyncio.sleep(1)
        rows = read_excel(path)
        for row in rows:
            await nc.publish("transaction.ingested", json.dumps(row).encode())
        
        # MOVE TO PROCESSED (Prevents duplication on refresh/restart)
        PROCESSED.mkdir(exist_ok=True)
        dest = PROCESSED / path_obj.name
        if dest.exists(): os.remove(dest)
        shutil.move(str(path_obj), str(dest))
        log.info(f"Moved {path_obj.name} to processed/")
    except Exception as e:
        log.error(f"Error processing {path_obj.name}: {e}")

class UploadHandler(FileSystemEventHandler):
    def __init__(self, loop, nc):
        self.loop = loop
        self.nc = nc

    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith(".xlsx"):
            asyncio.run_coroutine_threadsafe(emit_events(event.src_path, self.nc), self.loop)

async def main():
    nc = await nats.connect(NATS_URL)
    loop = asyncio.get_event_loop()
    
    for existing in UPLOADS.glob("*.xlsx"):
        await emit_events(str(existing), nc)

    observer = Observer()
    observer.schedule(UploadHandler(loop, nc), str(UPLOADS), recursive=False)
    observer.start()
    log.info(f"Watching {UPLOADS}...")
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
