import asyncio
import json
import logging
import os
from datetime import datetime, timedelta

import nats
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [timer] %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler("logs/timer.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

NATS_URL = os.getenv("NATS_URL", "nats://localhost:4222")
# APScheduler requires SQLAlchemy 1.x connection string (psycopg2, not psycopg3)
DSN_SYNC = os.getenv(
    "POSTGRES_DSN_SYNC",
    "postgresql+psycopg2://agent:agentpass@localhost:5432/payment_agent"
)

jobstores  = {"default": SQLAlchemyJobStore(url=DSN_SYNC)}
scheduler  = AsyncIOScheduler(jobstores=jobstores, timezone="UTC")


async def fire_reminder(transaction_id: str, retailer_id: str,
                        distributor_id: str, nats_url: str) -> None:
    """Called by APScheduler when 45 days have elapsed."""
    nc = await nats.connect(nats_url)
    payload = json.dumps({
        "secondary_transaction_id": transaction_id,
        "retailer_id": retailer_id,
        "distributor_id": distributor_id,
    })
    await nc.publish("reminder.due", payload.encode())
    await nc.close()
    log.info(f"Reminder fired for transaction {transaction_id}")


async def main():
    nc  = await nats.connect(NATS_URL)
    sub = await nc.subscribe("transaction.ingested")
    scheduler.start()
    log.info("Timer agent running — APScheduler started, subscribed to transaction.ingested")

    async for msg in sub.messages:
        try:
            row = json.loads(msg.data)
            txn_id   = row["secondary_transaction_id"]
            txn_date = datetime.strptime(row["transaction_date"], "%Y-%m-%d")
            fire_at  = txn_date + timedelta(days=45)
            job_id   = f"reminder_{txn_id}"

            # Don't re-schedule if job already exists
            if scheduler.get_job(job_id):
                log.debug(f"Job {job_id} already scheduled — skipping")
                continue

            scheduler.add_job(
                fire_reminder,
                trigger="date",
                run_date=fire_at,
                args=[txn_id, row["retailer_id"], row["distributor_id"], NATS_URL],
                id=job_id,
                replace_existing=False,
                misfire_grace_time=86400,  # fire up to 24h late if agent was down
            )
            log.info(
                f"Scheduled: {txn_id} | retailer={row['retailer_id']} "
                f"| fires={fire_at.date()}"
            )

        except Exception as e:
            log.error(f"Timer scheduling error: {e}", exc_info=True)


if __name__ == "__main__":
    asyncio.run(main())
