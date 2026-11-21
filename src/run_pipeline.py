import os
import subprocess
import sys
from pathlib import Path

import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent
SRC_DIR = BASE_DIR / "src"
TESTS_DIR = BASE_DIR / "tests"

GENERATE_SCRIPT = SRC_DIR / "generate_mock_data.py"
INGEST_SCRIPT = SRC_DIR / "ingest_to_bronze.py"


def resolve_test_script() -> Path:
    candidates = [
        TESTS_DIR / "regression_test.py",
        TESTS_DIR / "test_existing_pipeline_data.py",
        SRC_DIR / "regression_test.py",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate

    if TESTS_DIR.exists():
        py_tests = sorted(TESTS_DIR.glob("*.py"))
        if py_tests:
            return py_tests[0]

    raise FileNotFoundError(
        f"Could not find regression test script in {TESTS_DIR} or {SRC_DIR}"
    )


def run_script(script_path: Path, step_label: str) -> None:
    if not script_path.exists():
        raise FileNotFoundError(f"Missing required script for {step_label}: {script_path}")

    print(f"[{step_label}] Running {script_path.name}...")
    completed = subprocess.run(
        [sys.executable, str(script_path)],
        cwd=str(BASE_DIR),
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"{step_label} failed ({script_path.name}) with exit code {completed.returncode}"
        )
    print(f"[{step_label}] Completed successfully.")


def call_silver_sp() -> str:
    print("[3/4] Calling SILVER.SP_RAW_TO_STG()...")
    conn = snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "SKYPOINTS_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", "SKYPOINTS_DB"),
        role=os.getenv("SNOWFLAKE_ROLE"),
    )
    cur = conn.cursor()
    try:
        cur.execute("USE WAREHOUSE SKYPOINTS_WH")
        cur.execute("USE DATABASE SKYPOINTS_DB")
        cur.execute("CALL SILVER.SP_RAW_TO_STG()")
        row = cur.fetchone()
        return str(row[0]) if row else "SILVER.SP_RAW_TO_STG() completed"
    finally:
        cur.close()
        conn.close()


def main() -> None:
    test_script = resolve_test_script()
    run_script(GENERATE_SCRIPT, "1/4 Generate")
    run_script(INGEST_SCRIPT, "2/4 Ingest to Bronze")
    sp_message = call_silver_sp()
    print(f"[3/4] {sp_message}")
    run_script(test_script, "4/4 Regression Test")


if __name__ == "__main__":
    main()