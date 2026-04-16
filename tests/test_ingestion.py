import pytest
import duckdb
from pathlib import Path

FIXTURE = Path("tests/fixtures/sample_transactions.xlsx")
REQUIRED_COLUMNS = [
    "distributor_id", "retailer_id", "sku_name",
    "product_category_snapshot", "secondary_transaction_id",
    "transaction_date", "secondary_gross_value",
    "secondary_tax_amount", "secondary_net_value",
]


def test_excel_file_exists():
    assert FIXTURE.exists(), f"Fixture file not found: {FIXTURE}"


def test_duckdb_reads_excel():
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    cols = ", ".join(REQUIRED_COLUMNS)
    rows = con.execute(f"SELECT {cols} FROM read_xlsx('{FIXTURE}')").fetchall()
    assert len(rows) > 0, "No rows returned from Excel"


def test_all_required_columns_present():
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    df = con.execute(f"SELECT * FROM read_xlsx('{FIXTURE}')").df()
    for col in REQUIRED_COLUMNS:
        assert col in df.columns, f"Missing required column: {col}"


def test_no_null_transaction_ids():
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    rows = con.execute(
        f"SELECT secondary_transaction_id FROM read_xlsx('{FIXTURE}') "
        f"WHERE secondary_transaction_id IS NULL"
    ).fetchall()
    assert len(rows) == 0, "Found rows with NULL transaction IDs"


def test_numeric_values_are_positive():
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    rows = con.execute(
        f"SELECT secondary_gross_value, secondary_net_value FROM read_xlsx('{FIXTURE}') "
        f"WHERE secondary_gross_value < 0 OR secondary_net_value < 0"
    ).fetchall()
    assert len(rows) == 0, "Found negative monetary values"
