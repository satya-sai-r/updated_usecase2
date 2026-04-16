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
    format="%(asctime)s [escalation] %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler("logs/escalation.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

NATS_URL          = os.getenv("NATS_URL", "nats://localhost:4222")
DSN               = os.getenv("POSTGRES_DSN")
ESCALATION_DAYS   = int(os.getenv("ESCALATION_DAYS", "7"))
ESCALATION_HOUR   = int(os.getenv("ESCALATION_HOUR", "9"))


async def run_escalation_check() -> None:
    log.info("Running escalation check...")
    nc = await nats.connect(NATS_URL)

    async with await psycopg.AsyncConnection.connect(DSN) as db:
        # Find transactions where:
        # - reminder was sent
        # - no reply exists
        # - reminder was sent more than ESCALATION_DAYS ago
        rows = await db.execute("""
            SELECT t.secondary_transaction_id, t.retailer_id, t.distributor_id,
                   t.reminder_sent_at, t.reminder_count
            FROM transactions t
            LEFT JOIN payment_replies pr
                ON pr.transaction_id = t.secondary_transaction_id
            WHERE t.reminder_sent_at IS NOT NULL
              AND pr.id IS NULL
              AND t.reminder_sent_at < NOW() - INTERVAL '%s days'
              AND t.reminder_count < 3
        """, (ESCALATION_DAYS,))

        overdue = await rows.fetchall()
        log.info(f"Found {len(overdue)} overdue transactions")

        for row in overdue:
            txn_id, retailer_id, dist_id, sent_at, count = row
            payload = json.dumps({
                "secondary_transaction_id": txn_id,
                "retailer_id": retailer_id,
                "distributor_id": dist_id,
                "escalation": True,
                "reminder_count": count,
            })
            await nc.publish("reminder.due", payload.encode())
            log.info(f"Escalation re-triggered for {txn_id} (reminder #{count + 1})")

    await nc.close()


async def main():
    scheduler = AsyncIOScheduler(timezone="Asia/Kolkata")
    scheduler.add_job(
        run_escalation_check,
        trigger="cron",
        hour=ESCALATION_HOUR,
        minute=0,
        id="daily_escalation",
    )
    scheduler.start()
    log.info(f"Escalation agent running — daily check at {ESCALATION_HOUR}:00 IST")

    # Also run immediately on startup so you can test without waiting
    await run_escalation_check()

    await asyncio.Event().wait()  # run forever


if __name__ == "__main__":
    import selectors
    loop_factory = lambda: asyncio.SelectorEventLoop(selectors.SelectSelector())
    asyncio.run(main(), loop_factory=loop_factory)
