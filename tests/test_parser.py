import json
import pytest
import requests
from pathlib import Path

FIXTURES_PATH = Path("tests/fixtures/sample_replies.json")
DUCKLING_URL = "http://localhost:8000"


def duckling_available() -> bool:
    try:
        r = requests.get(f"{DUCKLING_URL}/", timeout=2)
        return r.status_code == 200
    except Exception:
        return False


def parse(text: str) -> dict:
    resp = requests.post(
        f"{DUCKLING_URL}/parse",
        data={"locale": "en_IN", "text": text,
              "dims": '["time","duration","amount-of-money"]'},
        timeout=5,
    )
    result = {}
    for e in resp.json():
        dim = e["dim"]
        val = e["value"]
        if dim == "time" and "date" not in result:
            result["date"] = val["value"][:10]
        elif dim == "duration" and "days" not in result:
            norm = val.get("normalized", {})
            result["days"] = round(norm.get("value", 0) / 86400)
        elif dim == "amount-of-money" and "amount" not in result:
            result["amount"] = val["value"]
    return result


@pytest.mark.skipif(not duckling_available(), reason="Duckling not running")
@pytest.mark.parametrize("fixture", json.loads(FIXTURES_PATH.read_text()) if FIXTURES_PATH.exists() else [])
def test_parser_extracts_expected_fields(fixture):
    extracted = parse(fixture["input"])
    expected  = fixture["expected"]
    for key, val in expected.items():
        assert key in extracted, f"Key '{key}' not extracted from: {fixture['input']}"
        if key == "date":
            assert extracted[key] == val, f"Date mismatch: {extracted[key]} != {val}"
        elif key == "days":
            assert abs(extracted[key] - val) <= 1, f"Days off by more than 1: {extracted[key]} vs {val}"


@pytest.mark.skipif(not duckling_available(), reason="Duckling not running")
def test_unparseable_returns_empty():
    result = parse("random unrelated text with no dates or amounts")
    assert result == {}, f"Expected empty dict, got: {result}"
