import asyncio
import json
import logging
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from datetime import datetime, timezone, timedelta

# IST timezone (UTC+5:30)
IST = timezone(timedelta(hours=5, minutes=30))

import nats
from dotenv import load_dotenv
from jinja2 import Environment, FileSystemLoader

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [email_dispatch] %(levelname)s %(message)s",
    handlers=[logging.FileHandler("logs/email_dispatch.log"), logging.StreamHandler()],
)
log = logging.getLogger(__name__)

STATE_FILE   = "data/system_state.json"
NATS_URL     = os.getenv("NATS_URL")
SMTP_HOST    = os.getenv("SMTP_HOST")
SMTP_PORT    = int(os.getenv("SMTP_PORT", "587"))
FROM_EMAIL   = os.getenv("FROM_EMAIL")
SMTP_PASS    = os.getenv("SMTP_PASS") or os.getenv("GMAIL_APP_PASSWORD")
RETAILER_MAP = json.loads(os.getenv("RETAILER_EMAIL_MAP", "{}"))
DEFAULT_RECIPIENT = os.getenv("DEFAULT_RECIPIENT", "spuvvala@gitam.in")

jinja = Environment(loader=FileSystemLoader("templates"))

def load_state():
    with open(STATE_FILE, "r") as f:
        return json.load(f)

def send_email(to_addr: str, subject: str, html_body: str) -> bool:
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = FROM_EMAIL
        msg["To"]      = to_addr
        msg.attach(MIMEText(html_body, "html"))
        
        server = smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=10)
        server.set_debuglevel(0)
        
        # SKIP TLS/LOGIN IF LOCALHOST (Mailpit)
        if SMTP_HOST != "localhost":
            server.starttls()
            server.login(FROM_EMAIL, SMTP_PASS)
            
        server.sendmail(FROM_EMAIL, to_addr, msg.as_string())
        server.quit()
        return True
    except Exception as e:
        log.error(f"SMTP Error: {e}")
        return False

async def handle_reminder(payload: dict, nc) -> None:
    txn_id = payload["secondary_transaction_id"]
    state = load_state()
    
    if txn_id not in state:
        log.warning(f"Txn {txn_id} not found in state file!")
        return

    data = state[txn_id]
    to_addr = RETAILER_MAP.get(data['retailer_id'], DEFAULT_RECIPIENT)
    
    template = jinja.get_template("reminder_email.html")
    
    # Construct a single item from the state data
    items = [{
        "sku_name": data.get("sku_name", "N/A"),
        "product_category_snapshot": "N/A",
        "secondary_gross_value": data['net_value'],
        "secondary_tax_amount": 0.0,
        "secondary_net_value": data['net_value']
    }]
    
    html = template.render(
        retailer_id=data['retailer_id'],
        distributor_id=data['distributor_id'],
        transaction_date=data['transaction_date'],
        items=items,
        total_tax=0.0,
        total_payable=data['net_value'],
        transaction_id=txn_id
    )
    
    subject = f"Payment Reminder — {data['distributor_id']} — {data['transaction_date']} [{txn_id}]"
    if send_email(to_addr, subject, html):
        log.info(f"SUCCESS: Email sent for {txn_id} to {to_addr}")
        # Notify that email was sent to update state/db
        sent_payload = {
            "transaction_id": txn_id,
            "sent_at": datetime.now(IST).isoformat()
        }
        await nc.publish("reminder.sent", json.dumps(sent_payload).encode())
    else:
        log.error(f"FAILURE: Email failed for {txn_id}")

async def main():
    nc = await nats.connect(NATS_URL)
    sub = await nc.subscribe("reminder.due")
    log.info("Email Dispatch Agent (v4.1) running...")
    async for msg in sub.messages:
        try:
            await handle_reminder(json.loads(msg.data), nc)
        except Exception as e:
            log.error(f"Loop error: {e}")

if __name__ == "__main__":
    import selectors
    loop_factory = lambda: asyncio.SelectorEventLoop(selectors.SelectSelector())
    asyncio.run(main(), loop_factory=loop_factory)
