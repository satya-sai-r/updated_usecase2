import re
import dateparser

def clean_body(body: str) -> str:
    lines = body.splitlines()
    clean_lines = []
    for line in lines:
        stripped = line.strip()
        # Stop if we hit the attribution line or quoted text
        if stripped.startswith(">") or re.match(r"^On .* wrote:", stripped, re.IGNORECASE):
            break
        if stripped: clean_lines.append(stripped)
    return " ".join(clean_lines)[:500]

def extract_date_from_attribution(body: str) -> str:
    match = re.search(r"On (.*?) <.*?> wrote:", body, re.IGNORECASE)
    if match:
        return match.group(1)
    return None

test_body = """I will pay in 10 days
On Tue, 7 Apr 2026 at 13:54 <routhsatyasai2004@gmail.com> wrote:
> Dear *R003*,
>
> This is a payment reminder from distributor *D02* for purchases made on
> *2026-03-31*.
"""

print(f"Original Body:\n{test_body}")
print("-" * 20)
cleaned = clean_body(test_body)
print(f"Cleaned Body: '{cleaned}'")

attr_date = extract_date_from_attribution(test_body)
print(f"Attribution Date: '{attr_date}'")
