import asyncio
import json
import logging
import os
from pathlib import Path
import pandas as pd
from datetime import datetime, timezone, timedelta
import tempfile
import time
import threading
import random

import nats
import psycopg
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [state_write] %(levelname)s %(message)s",
    handlers=[logging.FileHandler("logs/state_write.log"), logging.StreamHandler()],
)
log = logging.getLogger(__name__)

STATE_FILE = "data/system_state.json"
NATS_URL   = os.getenv("NATS_URL")
DSN        = os.getenv("POSTGRES_DSN")
OUTPUTS    = Path("outputs")

# Cross-platform file locking using portalocker for process-safe locks
try:
    import portalocker
    _file_lock = None  # Use portalocker instead
    def acquire_lock(f):
        portalocker.lock(f, portalocker.LOCK_EX)
    def release_lock(f):
        portalocker.unlock(f)
except ImportError:
    # Fallback to threading.Lock (only works within same process)
    _file_lock = threading.Lock()
    def acquire_lock(f):
        _file_lock.acquire()
    def release_lock(f):
        _file_lock.release()

def update_json_state_with_retry(txn_id, raw_body=None, received_at=None, promised_date=None, mail_sent_at=None, max_retries=5):
    """Update JSON state with exponential backoff retry for Windows file locking."""
    txn_id = str(txn_id)
    if not os.path.exists(STATE_FILE):
        log.error(f"State file missing: {STATE_FILE}")
        return False
    
    for attempt in range(max_retries):
        try:
            # Try to acquire exclusive lock on the state file itself
            with open(STATE_FILE, "r+") as lock_f:
                acquire_lock(lock_f)
                try:
                    # Read current state
                    lock_f.seek(0)
                    content = lock_f.read()
                    state = json.loads(content) if content else {}
                    
                    if txn_id not in state:
                        log.warning(f"Txn {txn_id} not found in JSON state during update")
                        return False
                    
                    # Update the transaction
                    if raw_body is not None:
                        state[txn_id]['reply_status'] = True
                        state[txn_id]['reply_content'] = raw_body
                    if received_at is not None:
                        state[txn_id]['replied_at'] = received_at
                    if promised_date is not None:
                        state[txn_id]['promised_date'] = promised_date
                    if mail_sent_at is not None:
                        state[txn_id]['mail_status'] = True
                        state[txn_id]['mail_sent_at'] = mail_sent_at
                    
                    # Write back atomically
                    lock_f.seek(0)
                    lock_f.truncate()
                    json.dump(state, lock_f, indent=2)
                    log.info(f"JSON state updated for Txn: {txn_id}")
                    return True
                    
                finally:
                    release_lock(lock_f)
                    
        except (IOError, OSError) as e:
            if attempt < max_retries - 1:
                wait_time = (2 ** attempt) + random.uniform(0, 0.1)  # Exponential backoff with jitter
                log.warning(f"File locked, retrying in {wait_time:.2f}s... (attempt {attempt + 1}/{max_retries})")
                time.sleep(wait_time)
            else:
                log.error(f"Failed to update JSON state for Txn {txn_id} after {max_retries} attempts: {e}")
                return False
        except Exception as e:
            log.error(f"Unexpected error updating JSON state for Txn {txn_id}: {e}")
            return False
    
    return False

def update_json_state(txn_id, raw_body=None, received_at=None, promised_date=None, mail_sent_at=None):
    """Legacy wrapper - now uses retry-enabled version."""
    return update_json_state_with_retry(txn_id, raw_body, received_at, promised_date, mail_sent_at)

async def update_db_sent(txn_id, sent_at, db):
    try:
        # First verify transaction exists
        result = await db.execute("""
            SELECT COUNT(*) as count FROM transactions 
            WHERE secondary_transaction_id = %s
        """, (txn_id,))
        count = await result.fetchone()
        
        if count[0] == 0:
            log.warning(f"Transaction {txn_id} not found in database - skipping update")
            return
        
        # Update the transaction
        await db.execute("""
            UPDATE transactions 
            SET reminder_sent_at = %s, reminder_count = reminder_count + 1
            WHERE secondary_transaction_id = %s
        """, (sent_at, txn_id))
        await db.commit()
        log.info(f"Database updated for Txn: {txn_id}")
    except Exception as e:
        log.error(f"DB update sent error: {e}")
        await db.rollback()

def append_to_excel(data: dict):
    dist_id = data.get("distributor_id", "Unknown")
    excel_path = OUTPUTS / f"{dist_id}_replies.xlsx"
    OUTPUTS.mkdir(exist_ok=True)

    new_row = {
        "retailer_id": data.get("retailer_id"),
        "distributor_id": dist_id,
        "transaction_id": data.get("transaction_id"),
        "reply_received_at": data.get("received_at"),
        "promised_date": data.get("date"),
        "promised_days": data.get("days"),
        "amount_confirmed": data.get("amount"),
        "raw_reply": data.get("raw_reply")
    }

    if excel_path.exists():
        df = pd.read_excel(excel_path)
        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
    else:
        df = pd.DataFrame([new_row])
    
    df.to_excel(excel_path, index=False)
    log.info(f"Excel updated: {excel_path}")

async def main():
    nc  = await nats.connect(NATS_URL)
    
    # Listen for parsed replies
    sub_parsed = await nc.subscribe("reply.parsed")
    # Listen for sent reminders
    sub_sent = await nc.subscribe("reminder.sent")
    
    log.info("State Write Agent (v2.2) running...")

    async with await psycopg.AsyncConnection.connect(DSN) as db:
        async def watch_parsed():
            async for msg in sub_parsed.messages:
                try:
                    data = json.loads(msg.data)
                    txn_id = data.get("transaction_id")
                    update_json_state(
                        txn_id, 
                        raw_body=data.get("raw_reply"), 
                        received_at=data.get("received_at"),
                        promised_date=data.get("date")
                    )
                    append_to_excel(data)
                except Exception as e:
                    log.error(f"State Parsed Error: {e}")

        async def watch_sent():
            async for msg in sub_sent.messages:
                try:
                    data = json.loads(msg.data)
                    txn_id = data.get("transaction_id")
                    sent_at = data.get("sent_at")
                    update_json_state(txn_id, mail_sent_at=sent_at)
                    await update_db_sent(txn_id, sent_at, db)
                except Exception as e:
                    log.error(f"State Sent Error: {e}")

        await asyncio.gather(watch_parsed(), watch_sent())

if __name__ == "__main__":
    import selectors
    loop_factory = lambda: asyncio.SelectorEventLoop(selectors.SelectSelector())
    asyncio.run(main(), loop_factory=loop_factory)
