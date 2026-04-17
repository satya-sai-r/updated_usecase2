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

def extract_day_durations_with_context(text: str) -> list[tuple[int, bool]]:
    """Extract all 'in X days' patterns with 'next' context.
    
    Returns list of (days, is_relative) tuples where is_relative indicates
    if the duration uses 'next' keyword (meaning relative to previous date).
    """
    pattern = r"in\s+(next\s+)?(\d+)\s+days?"
    matches = re.findall(pattern, text, re.IGNORECASE)
    return [(int(days), bool(next_keyword)) for next_keyword, days in matches]

def extract_absolute_date(text: str, received_at_str: str) -> int | None:
    """Extract absolute dates like 'on 27 april' and convert to days from now.
    
    Returns days from received_at to the absolute date, or None if not found.
    """
    # Pattern: "on 27 april" or "on April 27" or "on 27/04" or "on 27-04"
    patterns = [
        r"on\s+(\d{1,2})\s+(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*",  # "on 27 april"
        r"on\s+(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\s+(\d{1,2})",  # "on april 27"
        r"on\s+(\d{1,2})[/-](\d{1,2})",  # "on 27/04" or "on 27-04"
    ]
    
    text_lower = text.lower()
    current_year = datetime.now().year
    
    for pattern in patterns:
        match = re.search(pattern, text_lower)
        if match:
            try:
                if len(match.groups()) == 2:
                    if match.group(2).isdigit():
                        # DD/MM format: on 27/04
                        day, month = int(match.group(1)), int(match.group(2))
                    else:
                        # Month name format
                        month_str = match.group(2)[:3].lower()
                        month_map = {
                            'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
                            'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
                        }
                        month = month_map.get(month_str, 0)
                        day = int(match.group(1)) if match.group(1).isdigit() else int(match.group(2))
                    
                    if month > 0 and 1 <= day <= 31:
                        received_dt = datetime.fromisoformat(received_at_str)
                        target_date = datetime(current_year, month, day, 
                                               tzinfo=received_dt.tzinfo)
                        
                        # If date is in the past, assume next year
                        if target_date < received_dt:
                            target_date = datetime(current_year + 1, month, day,
                                                   tzinfo=received_dt.tzinfo)
                        
                        delta = target_date - received_dt
                        return delta.days
            except (ValueError, AttributeError):
                continue
    
    return None

def has_absolute_date_pattern(text: str) -> bool:
    """Check if text contains 'on [date]' pattern."""
    pattern = r"on\s+(\d{1,2}\s+(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)|\d{1,2}[/-]\d{1,2})"
    return bool(re.search(pattern, text.lower()))

def extract_month_end_date(text: str, received_at_str: str) -> int | None:
    """Extract 'month end' patterns and return days to last day of month.
    
    Handles: 'month end', 'month ends', 'end of month', 'before month ends'
    """
    month_end_patterns = [
        r"month\s+end[s]?",
        r"end\s+of\s+(?:the\s+)?month",
        r"before\s+(?:the\s+)?month\s+ends",
        r"by\s+(?:the\s+)?month\s+end",
    ]
    
    text_lower = text.lower()
    for pattern in month_end_patterns:
        if re.search(pattern, text_lower):
            try:
                received_dt = datetime.fromisoformat(received_at_str)
                # Get last day of current month
                if received_dt.month == 12:
                    last_day = datetime(received_dt.year + 1, 1, 1, tzinfo=received_dt.tzinfo) - timedelta(days=1)
                else:
                    last_day = datetime(received_dt.year, received_dt.month + 1, 1, tzinfo=received_dt.tzinfo) - timedelta(days=1)
                
                delta = last_day - received_dt
                return delta.days
            except (ValueError, AttributeError):
                continue
    
    return None

def has_month_end_pattern(text: str) -> bool:
    """Check if text contains month-end related pattern."""
    pattern = r"(month\s+end[s]?|end\s+of\s+(?:the\s+)?month|before\s+(?:the\s+)?month\s+ends|by\s+(?:the\s+)?month\s+end)"
    return bool(re.search(pattern, text.lower()))

def extract_next_month_date(text: str, received_at_str: str) -> int | None:
    """Extract 'Xth of next month' patterns and return days to that date.
    
    Handles: '5th of next month', 'before the 5th of next month', 
             'by 5th next month', 'on 5th of next month'
    """
    # Pattern to extract day number: 5th, 10th, 1st, etc.
    patterns = [
        r"(?:before|by|on)?\s*(?:the\s+)?(\d{1,2})(?:st|nd|rd|th)?\s+(?:of\s+)?next\s+month",
        r"(?:before|by|on)?\s*(?:the\s+)?(\d{1,2})(?:st|nd|rd|th)?\s+next\s+month",
    ]
    
    text_lower = text.lower()
    for pattern in patterns:
        match = re.search(pattern, text_lower)
        if match:
            try:
                day_of_month = int(match.group(1))
                if 1 <= day_of_month <= 31:
                    received_dt = datetime.fromisoformat(received_at_str)
                    
                    # Calculate next month and year
                    if received_dt.month == 12:
                        target_month = 1
                        target_year = received_dt.year + 1
                    else:
                        target_month = received_dt.month + 1
                        target_year = received_dt.year
                    
                    # Create target date
                    target_date = datetime(target_year, target_month, day_of_month, 
                                          tzinfo=received_dt.tzinfo)
                    
                    delta = target_date - received_dt
                    return delta.days
            except (ValueError, AttributeError):
                continue
    
    return None

def has_next_month_pattern(text: str) -> bool:
    """Check if text contains 'Xth of next month' pattern."""
    pattern = r"(\d{1,2})(?:st|nd|rd|th)?\s+(?:of\s+)?next\s+month"
    return bool(re.search(pattern, text.lower()))

def extract_weekend_date(text: str, received_at_str: str) -> int | None:
    """Extract 'weekend' patterns and return days to Saturday.
    
    Handles: 'by weekend', 'before weekend', 'this weekend', 'next weekend'
    """
    text_lower = text.lower()
    
    # Pattern for weekend references
    weekend_patterns = [
        r"(?:by|before|this)\s+weekend",
        r"next\s+weekend",
    ]
    
    for pattern in weekend_patterns:
        match = re.search(pattern, text_lower)
        if match:
            try:
                received_dt = datetime.fromisoformat(received_at_str)
                
                # Find days until Saturday (weekday 5)
                # Monday=0, Tuesday=1, ..., Friday=4, Saturday=5, Sunday=6
                days_until_sat = (5 - received_dt.weekday()) % 7
                
                if days_until_sat == 0:
                    # Today is Saturday, go to next Saturday
                    days_until_sat = 7
                
                # Check if "next weekend" was mentioned
                if "next" in match.group(0):
                    days_until_sat += 7
                
                return days_until_sat
            except (ValueError, AttributeError):
                continue
    
    return None

def has_weekend_pattern(text: str) -> bool:
    """Check if text contains weekend-related pattern."""
    pattern = r"(?:by|before|this|next)\s+weekend"
    return bool(re.search(pattern, text.lower()))

def extract_end_of_week(text: str, received_at_str: str) -> int | None:
    """Extract 'end of week' patterns and return days to Friday.
    
    Handles: 'end of this week', 'end of next week', 'by end of week'
    """
    text_lower = text.lower()
    
    # Pattern for end of week
    eow_patterns = [
        r"(?:by|before)?\s*end\s+of\s+(?:this\s+)?week",
        r"end\s+of\s+next\s+week",
    ]
    
    for pattern in eow_patterns:
        match = re.search(pattern, text_lower)
        if match:
            try:
                received_dt = datetime.fromisoformat(received_at_str)
                
                # Find days until Friday (weekday 4)
                days_until_fri = (4 - received_dt.weekday()) % 7
                
                if days_until_fri == 0:
                    # Today is Friday, this week ends today
                    # For "end of this week", assume today
                    # For "end of next week", add 7
                    if "next" in match.group(0):
                        days_until_fri = 7
                else:
                    # Check if "next week" was mentioned
                    if "next" in match.group(0):
                        days_until_fri += 7
                
                return days_until_fri if days_until_fri > 0 else 0
            except (ValueError, AttributeError):
                continue
    
    return None

def has_end_of_week_pattern(text: str) -> bool:
    """Check if text contains end-of-week pattern."""
    pattern = r"end\s+of\s+(?:this|next)?\s*week"
    return bool(re.search(pattern, text.lower()))

def has_installment_pattern(text: str) -> bool:
    """Check if text contains installment/multi-payment patterns."""
    installment_keywords = [
        r"installment[s]?",
        r"(?:\d+)\s+installment[s]?",
        r"(?:\d+)\s+(?:parts?|phases?|payments?)",
        r"first", r"second", r"third", r"fourth", r"fifth",
        r"1st", r"2nd", r"3rd", r"4th", r"5th",
    ]
    text_lower = text.lower()
    return any(re.search(pattern, text_lower) for pattern in installment_keywords)

def extract_all_durations(text: str, received_at_str: str) -> list[int]:
    """Extract all duration mentions from text and convert to days.
    
    Handles multiple installments by finding all time references.
    Returns list of days from received_at for each mention.
    """
    days_list = []
    
    # Pattern 1: "in X days" - extract all matches
    days_pattern = r"in\s+(?:next\s+)?(\d+)\s+days?"
    matches = re.findall(days_pattern, text, re.IGNORECASE)
    days_list.extend([int(m) for m in matches])
    
    # Pattern 2: Absolute dates (on 25 April, by 30th)
    # These are already handled by extract_absolute_date
    
    # Pattern 3: Weekdays (next Friday)
    # These are handled by Duckling
    
    return days_list

def is_split_payment(text: str) -> bool:
    """Detect if reply mentions partial/split payments with multiple dates."""
    text_lower = text.lower()
    has_multiple_days = len(extract_day_durations_with_context(text)) >= 2
    has_absolute_and_relative = has_absolute_date_pattern(text) and "next" in text_lower
    has_split_keywords = any(word in text_lower for word in ["half", "partial", "remaining", "balance", "rest", "next"])
    return (has_multiple_days or has_absolute_and_relative) and has_split_keywords

def calculate_final_pay_date(days_list: list[int], received_at_str: str) -> str:
    """Calculate final pay date from multiple durations.
    
    For split payments like:
    - 'half in 10 days and remaining in 20 days' -> use max(10, 20) = 20
    - 'half in 10 days and remaining in next 13 days' -> use 10 + 13 = 23
    """
    if not days_list:
        return None
    
    received_dt = datetime.fromisoformat(received_at_str)
    
    if len(days_list) == 1:
        final_dt = received_dt + timedelta(days=days_list[0])
    else:
        # For split payments, assume:
        # First number = first payment due
        # Second number = either absolute date OR relative days after first
        # Use the larger value (most conservative/final date)
        max_days = max(days_list)
        final_dt = received_dt + timedelta(days=max_days)
    
    return final_dt.strftime("%Y-%m-%d")

def parse_reply(text: str) -> dict:
    res = {"is_split_payment": is_split_payment(text)}
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
    
    # 2. Check for multiple "in X days" patterns (handles split payments)
    days_with_context = extract_day_durations_with_context(text)
    if days_with_context:
        all_days = [d for d, _ in days_with_context]
        res['all_days_found'] = all_days  # Store all found durations
        
        if len(days_with_context) > 1:
            # Calculate final date: if any duration uses "next", ADD to first date
            # otherwise use the MAX (absolute dates)
            first_days, _ = days_with_context[0]
            total_days = first_days
            
            for i, (days, is_relative) in enumerate(days_with_context[1:], start=1):
                if is_relative:
                    # "next X days" means X days AFTER the previous date
                    total_days += days
                else:
                    # Absolute date - take max with current total
                    total_days = max(total_days, days)
            
            res['days'] = total_days
            log.info(f"Split payment detected: {days_with_context}, final pay in: {res['days']} days")
        else:
            res['days'] = days_with_context[0][0]
    
    # 3. Fallback to dateparser if no days found
    if 'date' not in res and 'days' not in res:
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
                
                # Handle month-end pattern (e.g., "pay before month ends")
                if parsed.get('has_month_end') or has_month_end_pattern(body):
                    if 'date' not in parsed and 'days' not in parsed:
                        month_end_days = extract_month_end_date(body, received_at_str)
                        if month_end_days is not None:
                            parsed['days'] = month_end_days
                            parsed['has_month_end'] = True
                            log.info(f"Month-end pattern detected: pay in {month_end_days} days")
                
                # Handle "before the Xth of next month" pattern
                if has_next_month_pattern(body):
                    if 'date' not in parsed and 'days' not in parsed:
                        next_month_days = extract_next_month_date(body, received_at_str)
                        if next_month_days is not None:
                            parsed['days'] = next_month_days
                            parsed['has_next_month'] = True
                            log.info(f"Next-month pattern detected: pay in {next_month_days} days")
                
                # Handle "by weekend" pattern
                if has_weekend_pattern(body):
                    if 'date' not in parsed and 'days' not in parsed:
                        weekend_days = extract_weekend_date(body, received_at_str)
                        if weekend_days is not None:
                            parsed['days'] = weekend_days
                            parsed['has_weekend'] = True
                            log.info(f"Weekend pattern detected: pay in {weekend_days} days")
                
                # Handle "end of week" pattern
                if has_end_of_week_pattern(body):
                    if 'date' not in parsed and 'days' not in parsed:
                        eow_days = extract_end_of_week(body, received_at_str)
                        if eow_days is not None:
                            parsed['days'] = eow_days
                            parsed['has_end_of_week'] = True
                            log.info(f"End-of-week pattern detected: pay in {eow_days} days")
                
                # Handle multiple installments (3+ payments)
                if has_installment_pattern(body):
                    all_durations = extract_all_durations(body, received_at_str)
                    if len(all_durations) >= 2:
                        # Use the maximum (final installment date)
                        final_days = max(all_durations)
                        parsed['days'] = final_days
                        parsed['is_installment'] = True
                        parsed['all_installments'] = all_durations
                        log.info(f"Installment pattern detected: {len(all_durations)} payments, final in {final_days} days")
                
                # Handle "on [date] and next X days" pattern
                if parsed.get('is_split_payment') and has_absolute_date_pattern(body):
                    abs_days = extract_absolute_date(body, received_at_str)
                    rel_days_with_context = extract_day_durations_with_context(body)
                    
                    if abs_days is not None and rel_days_with_context:
                        # Get the relative "next X days" value
                        for days, is_rel in rel_days_with_context:
                            if is_rel:
                                total_days = abs_days + days
                                parsed['days'] = total_days
                                parsed['absolute_days'] = abs_days
                                parsed['relative_days'] = days
                                log.info(f"Absolute + Relative pattern: {abs_days} + {days} = {total_days} days")
                                break
                
                # Calculate date from days (especially important for split payments)
                if 'days' in parsed:
                    # If split payment detected, recalculate date from our total_days (more accurate)
                    # because Duckling only extracts the first date
                    if parsed.get('is_split_payment') or 'date' not in parsed:
                        try:
                            received_dt = datetime.fromisoformat(received_at_str)
                            promised_dt = received_dt + timedelta(days=int(parsed['days']))
                            parsed['date'] = promised_dt.strftime("%Y-%m-%d")
                            log.info(f"Calculated final pay date from {parsed['days']} days: {parsed['date']}")
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
