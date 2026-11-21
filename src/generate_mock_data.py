from __future__ import annotations

import argparse
import json
import os
import random
from datetime import date, datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv


VALID_LOCATIONS = [
    ("IND", "TN", ["600001", "641601", "620001", "625001"]),
    ("AUS", "NSW", ["2000", "2010", "2150", "2300"]),
    ("AU", "VIC", ["3000", "3004", "3121", "3141"]),
    ("USA", "CA", ["90001", "94105", "92101", "95112"]),
    ("PHIL", "NCR", ["1000", "1100", "1200", "1630"]),
    ("CAN", "ONT", ["M5V2T", "K1A0B", "L5B3C", "N2L3G"]),
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
    "Tier_Code|Agent_Name|State|Country|Post_Code|DOB|Is_Active"
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


def pick_last_flight_date(today: date, stale_ratio: float = 0.25) -> date:
    """Return a recent flight date (<90d) most of the time, or >90d based on stale_ratio."""
    if random.random() < stale_ratio:
        return today - timedelta(days=random.randint(91, 240))
    return today - timedelta(days=random.randint(0, 85))


def build_member_line(
    idx: int,
    today: date,
    base_id: int = 223457,
    stale_ratio: float = 0.25,
    force_bad: str | None = None,
    override_country: tuple[str, str, str] | None = None,
) -> str:
    member_id = make_member_id(idx, base_id=base_id)
    first_name = random.choice(FIRST_NAMES)
    last_name = random.choice(LAST_NAMES)
    member_name = f"{first_name} {last_name}"

    enroll_dt = random_date(2010, max(2010, today.year - 1))
    last_flight_dt = pick_last_flight_date(today, stale_ratio=stale_ratio)
    dob_dt = random_date(1965, 2002)

    if override_country:
        country, state, post_code = override_country
    else:
        country, state, post_codes = random.choice(VALID_LOCATIONS)
        post_code = random.choice(post_codes)

    tier_code = random.choice(TIERS)
    agent_name = random.choice(AGENTS)
    is_active = random.choice(["A", "A", "A", "I"])

    enroll_str = format_date_field(enroll_dt, "yyyymmdd")
    last_flight_str = format_date_field(last_flight_dt, "yyyymmdd")
    dob_str = format_date_field(dob_dt, "mmddyyyy")

    if force_bad == "bad_country_unknown":
        country, state, post_code = "XYZ", "NA", "00000"
    elif force_bad == "missing_name":
        member_name = ""
    elif force_bad == "missing_id":
        member_id = ""
    elif force_bad == "non_numeric_id":
        member_id = f"BAD-{idx}"
    elif force_bad == "duplicate_id":
        member_id = make_member_id(1, base_id=base_id)
    elif force_bad == "bad_date":
        enroll_str = "2021-13-13"
    elif force_bad == "future_date":
        last_flight_str = (today + timedelta(days=30)).strftime("%Y%m%d")
    elif force_bad == "malformed_pipe":
        # Mirrors the PDF sample anomaly "|D|Ravi 223458|..." (missing pipe delimiter)
        fields = [
            "D",
            f"{member_name} {member_id}",
            enroll_str,
            last_flight_str,
            tier_code,
            agent_name,
            state,
            country,
            post_code,
            dob_str,
            is_active,
        ]
        return "|" + "|".join(fields)

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
        post_code,
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
    stale_ratio: float = 0.25,
    first_member_country: tuple[str, str, str] | None = None,
) -> tuple[Path, Path]:
    flat_dir.mkdir(parents=True, exist_ok=True)
    json_dir.mkdir(parents=True, exist_ok=True)

    dt = run_date or date.today()
    date_str = dt.strftime("%Y%m%d")

    flat_path = flat_dir / f"member_profiles{date_str}.csv"
    json_path = json_dir / f"redemption_feed{date_str}.json"

    bad_modes_flat = [
        "bad_country_unknown",
        "missing_name",
        "missing_id",
        "non_numeric_id",
        "duplicate_id",
        "bad_date",
        "future_date",
        "malformed_pipe",
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

    bad_indices_flat = set(
        random.sample(candidate_indices, k=min(len(candidate_indices), quarantine_count))
    )
    bad_indices_json = set(
        random.sample(candidate_indices, k=min(len(candidate_indices), quarantine_count))
    )

    with flat_path.open("w", encoding="utf-8", newline="\n") as f_flat:
        f_flat.write(CSV_HEADER + "\n")
        for idx in range(1, count + 1):
            bad_mode = random.choice(bad_modes_flat) if idx in bad_indices_flat else None
            override = first_member_country if idx == 1 else None
            f_flat.write(
                build_member_line(
                    idx,
                    dt,
                    base_id=base_id,
                    stale_ratio=stale_ratio,
                    force_bad=bad_mode,
                    override_country=override,
                )
                + "\n"
            )

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
        description="Generate mock CSV member and JSON redemption records with <filename><YYYYMMDD> naming."
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
        "--stale-ratio",
        type=float,
        default=0.25,
        help="Fraction of members with >90d flight inactivity ('Y'); default 0.25 means ~75%% 'N'.",
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Optional date in YYYYMMDD format (defaults to today's date).",
    )
    parser.add_argument(
        "--country-move-demo",
        action="store_true",
        help="Generate two consecutive days where member 223457 moves from USA (Day 1) to IND (Day 2).",
    )
    args = parser.parse_args()

    parsed_date = datetime.strptime(args.date, "%Y%m%d").date() if args.date else date.today()
    if args.country_move_demo:
        day1 = parsed_date - timedelta(days=1)
        f1, j1 = generate_mock_files(
            flat_dir=args.flat_dir,
            json_dir=args.json_dir,
            count=args.count,
            run_date=day1,
            base_id=args.base_id,
            stale_ratio=args.stale_ratio,
            first_member_country=("USA", "CA", "94105"),
        )
        f2, j2 = generate_mock_files(
            flat_dir=args.flat_dir,
            json_dir=args.json_dir,
            count=args.count,
            run_date=parsed_date,
            base_id=args.base_id,
            stale_ratio=args.stale_ratio,
            first_member_country=("IND", "TN", "600001"),
        )
        print(f"Day 1 (USA -> member {args.base_id}): {f1}, {j1}")
        print(f"Day 2 (IND -> member {args.base_id}): {f2}, {j2}")
    else:
        flat_file, json_file = generate_mock_files(
            flat_dir=args.flat_dir,
            json_dir=args.json_dir,
            count=args.count,
            run_date=parsed_date,
            base_id=args.base_id,
            stale_ratio=args.stale_ratio,
        )
        print(f"Flat CSV dumped ({args.count} records) -> {flat_file}")
        print(f"JSON file dumped ({args.count} records) -> {json_file}")


if __name__ == "__main__":
    main()
