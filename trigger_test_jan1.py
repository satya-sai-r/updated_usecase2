import asyncio
import json
import logging
import nats
import psycopg
import os
import selectors
from dotenv import load_dotenv

load_dotenv()

DSN = os.getenv("POSTGRES_DSN")
NATS_URL = os.getenv("NATS_URL")

async def trigger():
    print("--- JAN 1ST BATCH TEST TRIGGER ---")
    nc = await nats.connect(NATS_URL)
    
    async with await psycopg.AsyncConnection.connect(DSN) as db:
        # 1. Pick ONE random retailer who has transactions on Jan 1st
        res = await db.execute("""
            SELECT retailer_id 
            FROM transactions 
            WHERE transaction_date = '2026-01-01'
            LIMIT 1
        """)
        row = await res.fetchone()
        if not row:
            print("ERROR: No transactions found for 2026-01-01. Did you ingest the file?")
            return
        
        retailer_id = row[0]
        
        # 2. Get all transactions for that retailer on Jan 1st
        res = await db.execute("""
            SELECT secondary_transaction_id, distributor_id
            FROM transactions
            WHERE retailer_id = %s AND transaction_date = '2026-01-01'
        """, (retailer_id,))
        txns = await res.fetchall()
        
        print(f"SELECTED RETAILER: {retailer_id}")
        print(f"FOUND {len(txns)} transactions on Jan 1st.")
        print(f"TRIGGERING {len(txns)} emails (1 per transaction)...")

        for txn in txns:
            txn_id, dist_id = txn
            payload = {
                "secondary_transaction_id": txn_id,
                "retailer_id": retailer_id,
                "distributor_id": dist_id
            }
            await nc.publish("reminder.due", json.dumps(payload).encode())
            print(f" [x] Triggered event for TXN: {txn_id}")

    await nc.close()
    print("--- TRIGGER COMPLETE ---")

if __name__ == "__main__":
    loop_factory = lambda: asyncio.SelectorEventLoop(selectors.SelectSelector())
    asyncio.run(trigger(), loop_factory=loop_factory)
