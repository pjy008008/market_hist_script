import unittest

import pandas as pd

from data_validation.audit_regular_session import audit_dataframe


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


if __name__ == "__main__":
    unittest.main()
