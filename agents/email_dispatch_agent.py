import asyncio
import json
import logging
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from datetime import datetime, timezone, timedelta
from collections import defaultdict

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

# Deduplication: Track recently sent emails to prevent duplicates
# Format: {(txn_id, email_type): timestamp}
recently_sent = defaultdict(float)
DEDUP_WINDOW_SECONDS = 30  # Deduplicate emails sent within 30 seconds

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
    additional_txns = payload.get("additional_transactions", [])
    is_escalation = payload.get("escalation", False)
    is_warning = payload.get("warning", False)
    reminder_count = payload.get("reminder_count", 0)
    state = load_state()

    # Collect all transaction IDs (primary + additional)
    all_txn_ids = [txn_id] + additional_txns

    # Check if primary transaction exists
    if txn_id not in state:
        log.warning(f"Txn {txn_id} not found in state file!")
        return

    # Determine email type
    if is_warning:
        email_type = "WARNING"
    elif is_escalation:
        email_type = "ESCALATION"
    else:
        email_type = "REMINDER"

    # Check if this email was recently sent (deduplication) - use primary txn_id
    current_time = datetime.now(IST).timestamp()
    key = (txn_id, email_type)
    if key in recently_sent:
        time_since_last = current_time - recently_sent[key]
        if time_since_last < DEDUP_WINDOW_SECONDS:
            log.info(f"SKIPPING: {email_type} email for {txn_id} already sent {time_since_last:.1f}s ago (within {DEDUP_WINDOW_SECONDS}s window)")
            return

    # Collect all transaction data
    items = []
    total_payable = 0.0
    retailer_id = None
    distributor_id = None
    transaction_date = None

    for tid in all_txn_ids:
        if tid in state:
            data = state[tid]
            items.append({
                "sku_name": data.get("sku_name", "N/A"),
                "product_category_snapshot": "N/A",
                "secondary_gross_value": data['net_value'],
                "secondary_tax_amount": 0.0,
                "secondary_net_value": data['net_value']
            })
            total_payable += data['net_value']

            # Use data from primary transaction for email metadata
            if tid == txn_id:
                retailer_id = data['retailer_id']
                distributor_id = data['distributor_id']
                transaction_date = data['transaction_date']

    to_addr = RETAILER_MAP.get(retailer_id, DEFAULT_RECIPIENT)

    # Format transaction IDs for subject line
    if len(all_txn_ids) > 1:
        txn_ids_str = f"{all_txn_ids[0]} + {len(all_txn_ids)-1} more"
    else:
        txn_ids_str = all_txn_ids[0]

    # Use warning template if warning flag is set, escalation template if escalation flag is set
    if is_warning:
        template = jinja.get_template("warning_email.html")
        subject = f"⚠️ FINAL WARNING: Supply Suspension Notice — {distributor_id} — {transaction_date} [{txn_ids_str}]"
    elif is_escalation:
        template = jinja.get_template("escalation_email.html")
        subject = f"⚠️ ESCALATION: Payment Follow-up — {distributor_id} — {transaction_date} [{txn_ids_str}]"
    else:
        template = jinja.get_template("reminder_email.html")
        subject = f"Payment Reminder — {distributor_id} — {transaction_date} [{txn_ids_str}]"

    html = template.render(
        retailer_id=retailer_id,
        distributor_id=distributor_id,
        transaction_date=transaction_date,
        items=items,
        total_tax=0.0,
        total_payable=total_payable,
        transaction_id=all_txn_ids[0],
        all_transaction_ids=all_txn_ids
    )

    if send_email(to_addr, subject, html):
        # Mark as recently sent
        recently_sent[key] = current_time
        log.info(f"SUCCESS: {email_type} email sent for {len(all_txn_ids)} transaction(s) [{', '.join(all_txn_ids)}] to {to_addr} (attempt #{reminder_count + 1})")
        # Notify that email was sent to update state/db for each transaction
        for tid in all_txn_ids:
            sent_payload = {
                "transaction_id": tid,
                "sent_at": datetime.now(IST).isoformat(),
                "escalation": is_escalation,
                "warning": is_warning,
                "reminder_count": reminder_count + 1
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
