import argparse
import json
import os
import random
from datetime import date, datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv


VALID_LOCATIONS = [
    ("IND", "TN"),
    ("AUS", "NSW"),
    ("USA", "CA"),
]

FIRST_NAMES = [
    "Elena", "Ravi", "Mateo", "Nora", "Arjun", "Priya", "Liam", "Chloe",
    "Karthik", "Ananya", "Noah", "Mia", "Vikram", "Diya", "Ethan", "Zoe",
    "Siddharth", "Meera", "Lucas", "Hannah", "Aditya", "Kavya", "Owen", "Tara",
]

LAST_NAMES = [
    "Nair", "Sharma", "Kulkarni", "Carter", "Bennett", "Krishnan",
    "Patel", "Reid", "Reddy", "Brooks", "Iyer", "Foster",
    "Menon", "Das", "Hayes", "Sullivan",
]

TIERS = ["GLD", "SLV", "PLT"]
AGENTS = ["Sam", "Asha", "Rohan", "Maya", "Liam"]
PARTNERS = ["AeroLink", "SkyStay Hotels", "CloudCab", "DutyFreePlus", "LoungePass"]
STATUSES = ["COMPLETED", "PENDING", "REVERSED"]

CSV_HEADER = (
    "|H|Member_Name|Member_Id|Enrollment_Date|Last_Flight_Date|"
    "Tier_Code|Agent_Name|State|Country|DOB|Is_Active"
)


def make_member_id(idx: int, base_id: int = 223457) -> str:
    """Deterministic numeric Member_Id shared between CSV and JSON feeds."""
    return str(base_id + idx - 1)


def format_date_field(dt: date, fmt_mode: str) -> str:
    if fmt_mode == "yyyymmdd":
        return dt.strftime("%Y%m%d")
    s = dt.strftime("%m%d%Y")
    if s.startswith("0") and random.random() < 0.25:
        return s[1:]  # e.g., "03051985" -> "3051985"
    return s


def random_date(start_year: int, end_year: int) -> date:
    start = date(start_year, 1, 1)
    end = date(end_year, 12, 28)
    delta_days = (end - start).days
    return start + timedelta(days=random.randint(0, max(1, delta_days)))


def build_member_line(
    idx: int,
    today: date,
    base_id: int = 223457,
    force_bad: str | None = None,
) -> str:
    member_id = make_member_id(idx, base_id=base_id)
    first_name = random.choice(FIRST_NAMES)
    last_name = random.choice(LAST_NAMES)
    member_name = f"{first_name} {last_name}"

    enroll_dt = random_date(2010, today.year - 1)
    last_flight_dt = random_date(2012, today.year)
    dob_dt = random_date(1965, 2002)

    country, state = random.choice(VALID_LOCATIONS)
    tier_code = random.choice(TIERS)
    agent_name = random.choice(AGENTS)
    is_active = random.choice(["A", "A", "A", "I"])

    enroll_str = format_date_field(enroll_dt, "yyyymmdd")
    last_flight_str = format_date_field(last_flight_dt, "yyyymmdd")
    dob_str = format_date_field(dob_dt, "mmddyyyy")

    if force_bad == "bad_country_phil":
        country, state = "PHIL", "NCR"
    elif force_bad == "bad_country_can":
        country, state = "CAN", "ONT"
    elif force_bad == "missing_name":
        member_name = ""
    elif force_bad == "missing_id":
        member_id = ""
    elif force_bad == "short_id":
        member_id = str(idx)
    elif force_bad == "non_numeric_id":
        member_id = f"BAD-{idx}"
    elif force_bad == "duplicate_id":
        member_id = make_member_id(1, base_id=base_id)
    elif force_bad == "bad_date":
        enroll_str = "2021-13-13"
    elif force_bad == "future_date":
        last_flight_str = (today + timedelta(days=30)).strftime("%Y%m%d")

    fields = [
        "D",
        member_name,
        member_id,
        enroll_str,
        last_flight_str,
        tier_code,
        agent_name,
        state,
        country,
        dob_str,
        is_active,
    ]
    return "|" + "|".join(fields)


def build_redemption_record(
    idx: int,
    run_date_str: str,
    today: date,
    base_id: int = 223457,
    force_bad: str | None = None,
) -> dict:
    member_id = make_member_id(idx, base_id=base_id)
    item_count = random.randint(1, 2)
    redemptions = []

    for j in range(item_count):
        txn_dt = today - timedelta(days=random.randint(1, 120))
        miles = random.choice([2500, 5000, 8500, 12000, 20000])
        status = random.choice(STATUSES)
        partner = random.choice(PARTNERS)
        txn_str = txn_dt.strftime("%Y%m%d")
        txn_id = f"RX-{run_date_str}-{idx:06d}-{j+1}"

        if force_bad == "bad_miles":
            miles = "NOT_A_NUMBER"
        elif force_bad == "future_txn_date":
            txn_str = (today + timedelta(days=15)).strftime("%Y%m%d")
        elif force_bad == "missing_txn_id":
            txn_id = ""

        redemptions.append(
            {
                "txn_id": txn_id,
                "txn_date": txn_str,
                "partner": partner,
                "miles_redeemed": miles,
                "status": status,
            }
        )

    record = {
        "member_id": member_id,
        "feed_date": run_date_str,
        "redemptions": redemptions,
    }

    if force_bad == "missing_member_id":
        record["member_id"] = ""
    elif force_bad == "non_numeric_member_id":
        record["member_id"] = f"INVALID-{idx}"
    elif force_bad == "bad_redemptions_type":
        record["redemptions"] = "INVALID_ARRAY"

    return record


def generate_mock_files(
    flat_dir: Path,
    json_dir: Path,
    count: int = 5000,
    run_date: date | None = None,
    base_id: int = 223457,
) -> tuple[Path, Path]:
    flat_dir.mkdir(parents=True, exist_ok=True)
    json_dir.mkdir(parents=True, exist_ok=True)

    dt = run_date or date.today()
    date_str = dt.strftime("%Y%m%d")

    flat_path = flat_dir / f"member_profiles{date_str}.csv"
    json_path = json_dir / f"redemption_feed{date_str}.json"

    bad_modes_flat = [
        "bad_country_phil",
        "bad_country_can",
        "missing_name",
        "missing_id",
        "short_id",
        "non_numeric_id",
        "duplicate_id",
        "bad_date",
        "future_date",
    ]
    bad_modes_json = [
        "bad_miles",
        "future_txn_date",
        "missing_txn_id",
        "missing_member_id",
        "non_numeric_member_id",
        "bad_redemptions_type",
    ]

    candidate_indices = list(range(2, count + 1)) if count > 1 else [1]
    quarantine_count = max(1, count // 100) if count >= 10 else 0

    bad_indices_flat = set(random.sample(candidate_indices, k=min(len(candidate_indices), quarantine_count)))
    bad_indices_json = set(random.sample(candidate_indices, k=min(len(candidate_indices), quarantine_count)))

    with flat_path.open("w", encoding="utf-8", newline="\n") as f_flat:
        f_flat.write(CSV_HEADER + "\n")
        for idx in range(1, count + 1):
            bad_mode = random.choice(bad_modes_flat) if idx in bad_indices_flat else None
            f_flat.write(build_member_line(idx, dt, base_id=base_id, force_bad=bad_mode) + "\n")

    json_records = []
    for idx in range(1, count + 1):
        bad_mode = random.choice(bad_modes_json) if idx in bad_indices_json else None
        json_records.append(
            build_redemption_record(idx, date_str, dt, base_id=base_id, force_bad=bad_mode)
        )

    with json_path.open("w", encoding="utf-8") as f_json:
        json.dump(json_records, f_json, indent=2)

    return flat_path, json_path


def main() -> None:
    load_dotenv()
    default_flat = Path(os.getenv("FLAT_DATA_FOLDER", "data/flat"))
    default_json = Path(os.getenv("JSON_DATA_FOLDER", "data/json"))

    parser = argparse.ArgumentParser(
        description="Generate 5,000 mock CSV member and JSON redemption records with <filename><YYYYMMDD> naming."
    )
    parser.add_argument(
        "--flat-dir",
        type=Path,
        default=default_flat,
        help="Folder to dump generated member CSV files (defaults to FLAT_DATA_FOLDER in .env).",
    )
    parser.add_argument(
        "--json-dir",
        type=Path,
        default=default_json,
        help="Folder to dump generated JSON redemption files (defaults to JSON_DATA_FOLDER in .env).",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=5000,
        help="Number of records per file (default: 5000).",
    )
    parser.add_argument(
        "--base-id",
        type=int,
        default=223457,
        help="Starting numeric Member_Id (default: 223457).",
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Optional date in YYYYMMDD format (defaults to today's date).",
    )
    args = parser.parse_args()

    parsed_date = datetime.strptime(args.date, "%Y%m%d").date() if args.date else date.today()
    flat_file, json_file = generate_mock_files(
        flat_dir=args.flat_dir,
        json_dir=args.json_dir,
        count=args.count,
        run_date=parsed_date,
        base_id=args.base_id,
    )
    print(f"Flat CSV dumped ({args.count} records) -> {flat_file}")
    print(f"JSON file dumped ({args.count} records) -> {json_file}")


if __name__ == "__main__":
    main()