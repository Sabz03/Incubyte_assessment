import unittest
from pathlib import Path
import tempfile

from src.ingest_to_bronze import (
    feed_date_from_filename,
    parse_member_file,
    sort_feed_files,
    unprocessed_feed_files,
)


class FeedFileSelectionTests(unittest.TestCase):
    def test_extracts_date_from_supported_filename_styles(self):
        self.assertEqual(
            feed_date_from_filename(Path("member_profiles20261121.csv")).isoformat(),
            "2026-11-21",
        )
        self.assertEqual(
            feed_date_from_filename(Path("redemption_feed_20261121.json")).isoformat(),
            "2026-11-21",
        )

    def test_rejects_missing_or_invalid_date(self):
        for filename in ("members.csv", "members_20260230.csv"):
            with self.subTest(filename=filename):
                with self.assertRaises(ValueError):
                    feed_date_from_filename(Path(filename))

    def test_sorts_chronologically_and_skips_loaded_filenames(self):
        paths = [
            Path("member_profiles_20261122.csv"),
            Path("member_profiles_20261120.csv"),
            Path("member_profiles_20261121.csv"),
        ]
        ordered = sort_feed_files(paths)
        pending = unprocessed_feed_files(ordered, {"member_profiles_20261121.csv"})

        self.assertEqual(
            [path.name for path in pending],
            ["member_profiles_20261120.csv", "member_profiles_20261122.csv"],
        )

    def test_parses_both_10_and_11_field_detail_layouts(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            member_file = Path(tmp_dir) / "members20261123.csv"
            member_file.write_text(
                "|H|Member_Name|Member_Id|Enrollment_Date|Last_Flight_Date|"
                "Tier_Code|Agent_Name|State|Country|Post_Code|DOB|Is_Active\n"
                "|D|Nora|22345|20101012|20121013|PLT|Sam|ONT|CAN|03051985|A\n"
                "|D|Jacob|2256|20101012|20121013|SLV|Sam|VIC|AU|3000|03051985|A\n",
                encoding="utf-8",
            )
            records = parse_member_file(member_file)

        self.assertEqual(records[0]["POST_CODE"], "")
        self.assertEqual(records[0]["DOB"], "03051985")
        self.assertEqual(records[1]["POST_CODE"], "3000")


if __name__ == "__main__":
    unittest.main()
