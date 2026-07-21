import tempfile
import unittest
from pathlib import Path

import pandas as pd

from data_validation.audit_regular_session import audit_dataframe, audit_source


class AuditRegularSessionTests(unittest.TestCase):
    @staticmethod
    def make_frame(timestamps: list[str]) -> pd.DataFrame:
        index = pd.MultiIndex.from_arrays(
            [
                ["AAPL"] * len(timestamps),
                pd.to_datetime(timestamps, utc=True),
            ],
            names=["symbol", "timestamp"],
        )
        return pd.DataFrame({"close": range(len(timestamps))}, index=index)

    def test_reports_missing_bar_between_observed_boundaries(self):
        dataframe = self.make_frame(
            [
                "2025-02-03 14:30:00+00:00",
                "2025-02-03 14:35:00+00:00",
                "2025-02-03 14:45:00+00:00",
            ]
        )

        summary, intervals = audit_dataframe(dataframe, "AAPL")

        self.assertEqual(summary["expected_bars"], 4)
        self.assertEqual(summary["missing_bars"], 1)
        self.assertEqual(summary["missing_intervals"], 1)
        self.assertEqual(summary["coverage_pct"], 75.0)
        self.assertEqual(intervals[0]["missing_start_utc"], "2025-02-03T14:40:00+00:00")
        self.assertEqual(intervals[0]["previous_bar_utc"], "2025-02-03T14:35:00+00:00")
        self.assertEqual(intervals[0]["next_bar_utc"], "2025-02-03T14:45:00+00:00")

    def test_does_not_expect_bars_outside_observed_date_range(self):
        dataframe = self.make_frame(
            [
                "2025-02-03 15:00:00+00:00",
                "2025-02-03 15:05:00+00:00",
            ]
        )

        summary, intervals = audit_dataframe(dataframe, "AAPL")

        self.assertEqual(summary["expected_bars"], 2)
        self.assertEqual(summary["missing_bars"], 0)
        self.assertEqual(intervals, [])

    def test_one_minute_sip_audit_uses_one_minute_frequency(self):
        dataframe = self.make_frame(
            [
                "2025-02-03 14:30:00+00:00",
                "2025-02-03 14:31:00+00:00",
                "2025-02-03 14:33:00+00:00",
            ]
        )

        summary, intervals = audit_dataframe(
            dataframe,
            "AAPL",
            bar_frequency=pd.Timedelta(minutes=1),
        )

        self.assertEqual(summary["expected_bars"], 4)
        self.assertEqual(summary["missing_bars"], 1)
        self.assertEqual(intervals[0]["missing_minutes"], 1)
        self.assertEqual(
            intervals[0]["missing_start_utc"],
            "2025-02-03T14:32:00+00:00",
        )

    def test_summary_only_counts_gaps_without_materializing_rows(self):
        dataframe = self.make_frame(
            [
                "2025-02-03 14:30:00+00:00",
                "2025-02-03 14:40:00+00:00",
                "2025-02-03 14:50:00+00:00",
            ]
        )

        summary, intervals = audit_dataframe(
            dataframe,
            "AAPL",
            include_intervals=False,
        )

        self.assertEqual(summary["missing_bars"], 2)
        self.assertEqual(summary["missing_intervals"], 2)
        self.assertEqual(intervals, [])

    def test_multi_interval_sip_filename_reports_only_ticker(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            source_dir = Path(temporary_directory)
            self.make_frame(
                [
                    "2025-02-03 14:30:00+00:00",
                    "2025-02-03 14:45:00+00:00",
                ]
            ).to_csv(
                source_dir / "AAPL_15min_sip_historical.csv",
                index=True,
            )

            summary, _ = audit_source(
                source_dir,
                "csv",
                "XNYS",
                pd.Timedelta(minutes=15),
                include_intervals=False,
            )

            self.assertEqual(summary.loc[0, "symbol"], "AAPL")


if __name__ == "__main__":
    unittest.main()
