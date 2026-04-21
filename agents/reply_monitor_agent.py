import asyncio
import email
import email.header
import email.utils
import imaplib
import json
import logging
import os
import re
import nats
from datetime import datetime, timezone, timedelta

# IST timezone (UTC+5:30)
IST = timezone(timedelta(hours=5, minutes=30))
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [reply_monitor] %(levelname)s %(message)s",
    handlers=[logging.FileHandler("logs/reply_monitor.log"), logging.StreamHandler()],
)
log = logging.getLogger(__name__)

IMAP_HOST = os.getenv("IMAP_HOST")
IMAP_USER = os.getenv("IMAP_USER")
IMAP_PASS = os.getenv("IMAP_PASS")
NATS_URL  = os.getenv("NATS_URL")

# Track processed emails to prevent duplicates
processed_emails = set()

def extract_id(subject):
    match = re.search(r"\[(.*?)\]", subject)
    if match:
        return match.group(1).strip().split('.')[0]
    return None

def poll_imap(queue: list):
    mail = None
    try:
        log.info(f"Connecting to IMAP {IMAP_HOST}...")
        mail = imaplib.IMAP4_SSL(IMAP_HOST)
        mail.login(IMAP_USER, IMAP_PASS)
        mail.select("INBOX")
        
        # Search for "Payment Reminder", "ESCALATION", and "WARNING" in subject
        _, msg_ids = mail.search(None, '(OR SUBJECT "Payment Reminder" (OR SUBJECT "ESCALATION" SUBJECT "WARNING"))')
        if msg_ids[0] is None:
            log.info("No messages found matching 'Payment Reminder', 'ESCALATION', or 'WARNING'")
            return
            
        id_list = msg_ids[0].split()
        log.info(f"Found {len(id_list)} messages matching 'Payment Reminder', 'ESCALATION', or 'WARNING'")
        
        processed_count = 0
        for msg_id in id_list:
            try:
                _, data = mail.fetch(msg_id, "(BODY.PEEK[])")
                if not data or not data[0]:
                    log.warning(f"Empty data for message ID {msg_id}")
                    continue
                    
                raw_email = data[0][1]
                msg = email.message_from_bytes(raw_email)
                
                # Create unique identifier for this email
                email_id = f"{msg_id}_{msg.get('Message-ID', '')}"
                
                # 1. Extract and Decode Subject
                subject_raw = msg.get("Subject")
                if not subject_raw: 
                    log.debug(f"No subject for message ID {msg_id}")
                    continue
                    
                decoded_parts = email.header.decode_header(subject_raw)
                subject = ""
                for part, encoding in decoded_parts:
                    if isinstance(part, bytes):
                        subject += part.decode(encoding or "utf-8", errors="replace")
                    else:
                        subject += part
                
                txn_id = extract_id(subject)
                if not txn_id: 
                    log.debug(f"No transaction ID in subject: {subject}")
                    continue
                
                # Check if this email has already been processed
                if email_id in processed_emails:
                    log.debug(f"Skipping already processed email: {email_id}")
                    continue

                # 2. Extract ACTUAL Reply Date from Email Headers
                date_str = msg.get("Date")
                if date_str:
                    try:
                        parsed_date = email.utils.parsedate_to_datetime(date_str)
                        received_at = parsed_date.isoformat()
                    except Exception as e:
                        log.warning(f"Failed to parse date '{date_str}': {e}")
                        received_at = datetime.now(IST).isoformat()
                else:
                    received_at = datetime.now(IST).isoformat()
                
                # 3. Extract and Clean Body (Reply Content)
                body = ""
                try:
                    if msg.is_multipart():
                        for part in msg.walk():
                            if part.get_content_type() == "text/plain":
                                payload = part.get_payload(decode=True)
                                if payload: 
                                    body = payload.decode(errors="replace")
                                break
                    else:
                        payload = msg.get_payload(decode=True)
                        if payload: 
                            body = payload.decode(errors="replace")
                except Exception as e:
                    log.warning(f"Failed to extract body from {msg_id}: {e}")
                    body = ""
                
                # CLEANING: Keep only the new content, stop at "On ... wrote:" or other markers
                lines = body.splitlines()
                clean_lines = []
                
                # Common patterns for attribution headers
                attribution_patterns = [
                    r"^On\s+.*wrote:\s*$",
                    r"^-+Original Message-+$",
                    r"^From:\s+.*",
                    r"^Sent:\s+.*",
                    r"^To:\s+.*",
                    r"^Subject:\s+.*"
                ]
                
                for i, line in enumerate(lines):
                    stripped = line.strip()
                    if not stripped: continue
                    
                    # Check for start of attribution block
                    is_attribution = False
                    for pattern in attribution_patterns:
                        if re.match(pattern, stripped, re.IGNORECASE):
                            is_attribution = True
                            break
                    
                    # Multi-line attribution check: line starts with "On " and some subsequent line has "wrote:"
                    if not is_attribution and stripped.startswith("On "):
                        # Look ahead a few lines for "wrote:"
                        for j in range(i, min(i + 5, len(lines))):
                            if "wrote:" in lines[j].lower():
                                is_attribution = True
                                break
                    
                    if is_attribution:
                        break
                    
                    # Extract content, stripping leading '>' but preserving the message
                    content = stripped.lstrip(">").strip()
                    if content:
                        clean_lines.append(content)
                
                clean_body = " ".join(clean_lines)[:500]
                
                queue.append({
                    "transaction_id": txn_id,
                    "body": clean_body,
                    "received_at": received_at
                })
                
                # Mark this email as processed
                processed_emails.add(email_id)
                processed_count += 1
                log.info(f"CAPTURED: Txn {txn_id} | Sent at: {received_at}")
                
            except Exception as e:
                log.error(f"Error processing message {msg_id}: {e}")
                continue
        
        log.info(f"Successfully processed {processed_count} new emails")
        
    except imaplib.IMAP4.error as e:
        log.error(f"IMAP protocol error: {e}")
    except ConnectionError as e:
        log.error(f"IMAP connection error: {e}")
    except Exception as e:
        log.error(f"Unexpected IMAP error: {e}")
    finally:
        if mail:
            try:
                mail.logout()
            except (imaplib.IMAP4.error, ConnectionError, OSError):
                pass

async def poll_imap_with_retry(queue: list, max_retries=3):
    for attempt in range(max_retries):
        try:
            poll_imap(queue)
            if queue:  # If we got any results, return immediately
                return
        except Exception as e:
            if attempt == max_retries - 1:
                log.error(f"IMAP polling failed after {max_retries} attempts: {e}")
                return
            
            wait_time = 2 ** attempt  # Exponential backoff: 1, 2, 4 seconds
            log.warning(f"IMAP polling attempt {attempt + 1} failed: {e}. Retrying in {wait_time}s...")
            await asyncio.sleep(wait_time)

async def main():
    nc = await nats.connect(NATS_URL)
    log.info("Reply Monitor v5.2 (with retry mechanism) active...")
    while True:
        queue = []
        await poll_imap_with_retry(queue)
        for item in queue:
            await nc.publish("reply.received", json.dumps(item).encode())
        await asyncio.sleep(15)

if __name__ == "__main__":
    asyncio.run(main())
