import asyncio
import json
import logging
import os
from datetime import datetime

import nats
import psycopg
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [warning] %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler("logs/warning.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

NATS_URL = os.getenv("NATS_URL", "nats://localhost:4222")
DSN = os.getenv("POSTGRES_DSN")
WARNING_MINUTES = int(os.getenv("WARNING_MINUTES", "1"))  # 1 minute after escalation


async def run_warning_check() -> None:
    log.info(f"Running {WARNING_MINUTES}-minute warning check after escalation...")
    nc = await nats.connect(NATS_URL)

    async with await psycopg.AsyncConnection.connect(DSN) as db:
        # Find transactions where:
        # - escalation was sent
        # - no reply exists
        # - escalation was sent more than WARNING_MINUTES ago (default 1 minute)
        # - warning has not been sent yet
        rows = await db.execute("""
            SELECT t.secondary_transaction_id, t.retailer_id, t.distributor_id,
                   t.escalation_sent_at, t.reminder_count
            FROM transactions t
            LEFT JOIN payment_replies pr
                ON pr.transaction_id = t.secondary_transaction_id
            WHERE t.escalation_sent_at IS NOT NULL
              AND t.warning_sent_at IS NULL
              AND pr.id IS NULL
              AND t.escalation_sent_at < NOW() - (INTERVAL '1 minute' * %s)
        """, (WARNING_MINUTES,))

        overdue = await rows.fetchall()
        log.info(f"Found {len(overdue)} transactions with no reply {WARNING_MINUTES} minutes after escalation")

        for row in overdue:
            txn_id, retailer_id, dist_id, sent_at, count = row
            payload = json.dumps({
                "secondary_transaction_id": txn_id,
                "retailer_id": retailer_id,
                "distributor_id": dist_id,
                "warning": True,
                "reminder_count": count,
            })
            await nc.publish("reminder.due", payload.encode())
            log.info(f"Warning email triggered for {txn_id} (no reply {WARNING_MINUTES} minutes after escalation)")

    await nc.close()


async def main():
    scheduler = AsyncIOScheduler(timezone="Asia/Kolkata")
    scheduler.add_job(
        run_warning_check,
        trigger="interval",
        minutes=1,  # Run every 1 minute
        id="minute_warning",
    )
    scheduler.start()
    log.info(f"Warning agent running — checking every 1 minute for no-reply after escalation (5 min threshold)")

    # Also run immediately on startup so you can test without waiting
    await run_warning_check()

    await asyncio.Event().wait()  # run forever


if __name__ == "__main__":
    import selectors
    loop_factory = lambda: asyncio.SelectorEventLoop(selectors.SelectSelector())
    asyncio.run(main(), loop_factory=loop_factory)
