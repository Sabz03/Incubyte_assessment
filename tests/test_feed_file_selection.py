import unittest
from pathlib import Path

from src.ingest_to_bronze import (
    feed_date_from_filename,
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


if __name__ == "__main__":
    unittest.main()
