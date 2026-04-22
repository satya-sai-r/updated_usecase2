import asyncio
import json
import logging
import os
from datetime import datetime
from pathlib import Path

import nats
import psycopg
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [escalation] %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler("logs/escalation.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

NATS_URL          = os.getenv("NATS_URL", "nats://localhost:4222")
DSN               = os.getenv("POSTGRES_DSN")
ESCALATION_MINUTES = int(os.getenv("ESCALATION_MINUTES", "5"))  # Changed to minutes for quick escalation
STATE_FILE = "data/system_state.json"

def load_state():
    """Load JSON state file."""
    state_path = Path(STATE_FILE)
    if not state_path.exists():
        return {}
    with open(state_path, "r") as f:
        return json.load(f)


# OLD DAILY ESCALATION LOGIC (COMMENTED OUT)
# async def run_escalation_check() -> None:
#     log.info("Running escalation check...")
#     nc = await nats.connect(NATS_URL)
# 
#     async with await psycopg.AsyncConnection.connect(DSN) as db:
#         # Find transactions where:
#         # - reminder was sent
#         # - no reply exists
#         # - reminder was sent more than ESCALATION_DAYS ago
#         rows = await db.execute("""
#             SELECT t.secondary_transaction_id, t.retailer_id, t.distributor_id,
#                    t.reminder_sent_at, t.reminder_count
#             FROM transactions t
#             LEFT JOIN payment_replies pr
#                 ON pr.transaction_id = t.secondary_transaction_id
#             WHERE t.reminder_sent_at IS NOT NULL
#               AND pr.id IS NULL
#               AND t.reminder_sent_at < NOW() - INTERVAL '%s days'
#               AND t.reminder_count < 3
#         """, (ESCALATION_DAYS,))
# 
#         overdue = await rows.fetchall()
#         log.info(f"Found {len(overdue)} overdue transactions")
# 
#         for row in overdue:
#             txn_id, retailer_id, dist_id, sent_at, count = row
#             payload = json.dumps({
#                 "secondary_transaction_id": txn_id,
#                 "retailer_id": retailer_id,
#                 "distributor_id": dist_id,
#                 "escalation": True,
#                 "reminder_count": count,
#             })
#             await nc.publish("reminder.due", payload.encode())
#             log.info(f"Escalation re-triggered for {txn_id} (reminder #{count + 1})")
# 
#     await nc.close()

# NEW 5-MINUTE ESCALATION LOGIC
async def run_escalation_check() -> None:
    log.info("Running 5-minute escalation check...")
    nc = await nats.connect(NATS_URL)

    async with await psycopg.AsyncConnection.connect(DSN) as db:
        # Find transactions where:
        # - reminder was sent
        # - no reply exists
        # - reminder was sent more than ESCALATION_MINUTES ago (default 5 minutes)
        # - escalation has not been sent yet (escalation_sent_at is NULL)
        rows = await db.execute("""
            SELECT t.secondary_transaction_id, t.retailer_id, t.distributor_id,
                   t.reminder_sent_at, t.reminder_count
            FROM transactions t
            LEFT JOIN payment_replies pr
                ON pr.transaction_id = t.secondary_transaction_id
            WHERE t.reminder_sent_at IS NOT NULL
              AND pr.id IS NULL
              AND t.escalation_sent_at IS NULL
              AND t.reminder_sent_at < NOW() - (INTERVAL '1 minute' * %s)
        """, (ESCALATION_MINUTES,))

        overdue = await rows.fetchall()
        log.info(f"Found {len(overdue)} transactions with no reply for {ESCALATION_MINUTES} minutes")

        # Load state to check for grouped transactions
        state = load_state()
        processed_groups = set()  # Track which groups have been processed

        for row in overdue:
            txn_id, retailer_id, dist_id, sent_at, count = row

            # Check if this transaction is part of a group
            grouped_txns = state.get(txn_id, {}).get('grouped_transactions', [])

            if grouped_txns:
                # Check if this group has already been processed
                group_key = tuple(sorted(grouped_txns))
                if group_key in processed_groups:
                    log.info(f"Skipping {txn_id} - group already processed")
                    continue
                processed_groups.add(group_key)

                # Send escalation for all transactions in the group
                payload = json.dumps({
                    "secondary_transaction_id": grouped_txns[0],
                    "retailer_id": retailer_id,
                    "distributor_id": dist_id,
                    "escalation": True,
                    "reminder_count": count,
                    "additional_transactions": grouped_txns[1:]
                })
                await nc.publish("reminder.due", payload.encode())
                log.info(f"Escalation email triggered for group of {len(grouped_txns)} transactions: {', '.join(grouped_txns)} (no reply for {ESCALATION_MINUTES} minutes)")
            else:
                # Send escalation for single transaction
                payload = json.dumps({
                    "secondary_transaction_id": txn_id,
                    "retailer_id": retailer_id,
                    "distributor_id": dist_id,
                    "escalation": True,
                    "reminder_count": count,
                })
                await nc.publish("reminder.due", payload.encode())
                log.info(f"Escalation email triggered for {txn_id} (no reply for {ESCALATION_MINUTES} minutes - reminder #{count + 1})")

    await nc.close()


async def main():
    scheduler = AsyncIOScheduler(timezone="Asia/Kolkata")
    scheduler.add_job(
        run_escalation_check,
        trigger="interval",
        minutes=1,  # Run every 1 minute
        id="minute_escalation",
    )
    scheduler.start()
    log.info(f"Escalation agent running — checking every 1 minute for no-reply escalation (5 min threshold)")

    # Also run immediately on startup so you can test without waiting
    await run_escalation_check()

    await asyncio.Event().wait()  # run forever


if __name__ == "__main__":
    import selectors
    loop_factory = lambda: asyncio.SelectorEventLoop(selectors.SelectSelector())
    asyncio.run(main(), loop_factory=loop_factory)
