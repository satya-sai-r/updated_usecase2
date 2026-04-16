import asyncio
import json
import logging
import os
import re
import nats
import requests
import dateparser
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [reply_parser] %(levelname)s %(message)s",
    handlers=[logging.FileHandler("logs/reply_parser.log"), logging.StreamHandler()],
)
log = logging.getLogger(__name__)

NATS_URL     = os.getenv("NATS_URL")
DUCKLING_URL = os.getenv("DUCKLING_URL")
STATE_FILE   = "data/system_state.json"

def load_state():
    with open(STATE_FILE, "r") as f:
        return json.load(f)

def parse_reply(text: str) -> dict:
    res = {}
    # 1. Attempt Duckling
    try:
        resp = requests.post(f"{DUCKLING_URL}/parse", data={"locale": "en_IN", "text": text, "dims": '["time","duration","amount-of-money"]'}, timeout=5)
        entities = resp.json()
        for e in entities:
            if e['dim'] == 'time': 
                res['date'] = e['value']['value'][:10]
            if e['dim'] == 'duration':
                # Convert duration to days if possible
                val = e['value']
                if val.get('unit') == 'day':
                    res['days'] = val['value']
                elif 'normalized' in val:
                    res['days'] = round(val['normalized']['value'] / 86400)
            if e['dim'] == 'amount-of-money': 
                res['amount'] = e['value']['value']
    except (requests.RequestException, KeyError, ValueError):
        pass
    
    # 2. Fallback to dateparser if date/days not found
    if 'date' not in res and 'days' not in res:
        # Check for "in X days" manually as a simple fallback
        match = re.search(r"in\s+(\d+)\s+days?", text, re.IGNORECASE)
        if match:
            res['days'] = int(match.group(1))
        else:
            parsed_date = dateparser.parse(text, settings={'PREFER_DATES_FROM': 'future'})
            if parsed_date:
                res['date'] = parsed_date.strftime("%Y-%m-%d")
            
    return res

async def main():
    nc = await nats.connect(NATS_URL)
    sub = await nc.subscribe("reply.received")
    log.info("Reply Parser Agent running...")

    async for msg in sub.messages:
        try:
            payload = json.loads(msg.data)
            txn_id = payload['transaction_id']
            state = load_state()
            
            if txn_id in state:
                meta = state[txn_id]
                body = payload['body']
                received_at_str = payload['received_at']
                
                parsed = parse_reply(body)
                
                # If we have days but no date, calculate it from received_at
                if 'days' in parsed and 'date' not in parsed:
                    try:
                        # received_at is ISO format from reply_monitor
                        received_dt = datetime.fromisoformat(received_at_str)
                        promised_dt = received_dt + timedelta(days=int(parsed['days']))
                        parsed['date'] = promised_dt.strftime("%Y-%m-%d")
                    except Exception as e:
                        log.error(f"Date calculation error: {e}")

                result = {
                    "transaction_id": txn_id,
                    "retailer_id": meta['retailer_id'],
                    "distributor_id": meta['distributor_id'],
                    "raw_reply": body,
                    "received_at": received_at_str,
                    **parsed
                }
                await nc.publish("reply.parsed", json.dumps(result).encode())
                log.info(f"PARSED: {txn_id} -> {parsed.get('date', 'no date')}")
        except Exception as e:
            log.error(f"Parser error: {e}")

if __name__ == "__main__":
    import selectors
    loop_factory = lambda: asyncio.SelectorEventLoop(selectors.SelectSelector())
    asyncio.run(main(), loop_factory=loop_factory)
