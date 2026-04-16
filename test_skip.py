import re

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

test_body = "> I will pay in 10 days"
print(f"Body: '{test_body}'")
print(f"Cleaned: '{clean_body(test_body)}'")
