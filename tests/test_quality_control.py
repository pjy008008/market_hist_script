import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from data_collection.collect_sip_1min import save_local_data
from data_validation.quality_control import (
    invalid_market_rows,
    repair_symbol_file,
    repair_windows,
)


class QualityControlTests(unittest.TestCase):
    @staticmethod
    def make_frame(timestamps: list[str], closes: list[float]) -> pd.DataFrame:
        index = pd.MultiIndex.from_arrays(
            [
                ["AAPL"] * len(timestamps),
                pd.to_datetime(timestamps, utc=True),
            ],
            names=["symbol", "timestamp"],
        )
        return pd.DataFrame(
            {
                "open": closes,
                "high": [value + 1 for value in closes],
                "low": [value - 1 for value in closes],
                "close": closes,
                "volume": [100] * len(timestamps),
                "trade_count": [10] * len(timestamps),
                "vwap": closes,
            },
            index=index,
        )

    def test_invalid_ohlcv_rows_report_price_and_volume_errors(self):
        dataframe = self.make_frame(["2025-02-03 14:30:00Z"], [100.0])
        dataframe.loc[:, "high"] = 99.0
        dataframe.loc[:, "volume"] = -1

        rows = invalid_market_rows(dataframe, "AAPL")

        self.assertEqual(len(rows), 1)
        self.assertIn("high_below_ohlc", rows[0]["reasons"])
        self.assertIn("negative_or_invalid_volume", rows[0]["reasons"])

    def test_repair_windows_only_include_recent_sessions(self):
        intervals = [
            {
                "session_date": "2025-01-02",
                "missing_start_utc": "2025-01-02T14:31:00+00:00",
                "missing_end_utc": "2025-01-02T14:31:00+00:00",
            },
            {
                "session_date": "2025-02-03",
                "missing_start_utc": "2025-02-03T14:31:00+00:00",
                "missing_end_utc": "2025-02-03T14:33:00+00:00",
            },
        ]

        windows = repair_windows(
            intervals, datetime(2025, 2, 1, tzinfo=timezone.utc)
        )

        self.assertEqual(
            windows,
            [
                (
                    datetime(2025, 2, 3, 14, 31, tzinfo=timezone.utc),
                    datetime(2025, 2, 3, 14, 34, tzinfo=timezone.utc),
                )
            ],
        )

    def test_recent_missing_bar_is_fetched_and_merged(self):
        existing = self.make_frame(
            ["2025-02-03 14:30:00Z", "2025-02-03 14:32:00Z"],
            [100.0, 102.0],
        )
        repaired = self.make_frame(["2025-02-03 14:31:00Z"], [101.0])
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "AAPL.csv"
            save_local_data(existing, path, "csv")
            with patch(
                "data_validation.quality_control.fetch_range",
                return_value=[repaired],
            ):
                result = repair_symbol_file(
                    object(),
                    "AAPL",
                    path,
                    "csv",
                    "adjusted",
                    datetime(2025, 2, 3, 14, 30, tzinfo=timezone.utc),
                    datetime(2025, 2, 3, 14, 33, tzinfo=timezone.utc),
                    datetime(2025, 2, 3, 14, 30, tzinfo=timezone.utc),
                    7,
                    0,
                )

        self.assertTrue(result.success)
        self.assertEqual(result.repaired_rows, 1)
        self.assertEqual(result.summary["missing_bars"], 0)
        self.assertEqual(
            result.changed_from_utc,
            pd.Timestamp("2025-02-03 14:31:00+00:00"),
        )


if __name__ == "__main__":
    unittest.main()

