import re

def clean_body(body: str) -> str:
    lines = body.splitlines()
    clean_lines = []
    
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
        
        is_attribution = False
        for pattern in attribution_patterns:
            if re.match(pattern, stripped, re.IGNORECASE):
                is_attribution = True
                break
        
        if not is_attribution and stripped.startswith("On "):
            for j in range(i, min(i + 5, len(lines))):
                if "wrote:" in lines[j].lower():
                    is_attribution = True
                    break
        
        if is_attribution:
            break
        
        content = stripped.lstrip(">").strip()
        if content:
            clean_lines.append(content)
    
    return " ".join(clean_lines)[:500]

test_body = """ > I will pay in 10 days
   On Tue, 7 Apr 2026 at 13:54
   remaining not need
   <routhsatyasai2004@gmail.com> wrote:
   > Dear *R003*,
   >
   > This is a payment reminder...
"""

print(f"Body:\n{test_body}")
print("-" * 20)
cleaned = clean_body(test_body)
print(f"Cleaned: '{cleaned}'")
