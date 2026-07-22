import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd

from data_collection.collect_sip_long_term import (
    BASE_TIMEFRAME_MINUTES,
    OUTPUT_INTERVALS,
    build_output_bars,
    fetch_chunk,
    output_paths,
    rolling_window,
)


class CollectSipLongTermTests(unittest.TestCase):
    @staticmethod
    def make_thirty_minute_frame() -> pd.DataFrame:
        timestamps = pd.date_range(
            "2025-02-03 09:00:00+00:00",
            "2025-02-03 22:00:00+00:00",
            freq="30min",
        )
        index = pd.MultiIndex.from_arrays(
            [["AAPL"] * len(timestamps), timestamps],
            names=["symbol", "timestamp"],
        )
        values = list(range(len(timestamps)))
        return pd.DataFrame(
            {
                "open": [100 + value for value in values],
                "high": [101 + value for value in values],
                "low": [99 + value for value in values],
                "close": [100.5 + value for value in values],
                "volume": [100] * len(timestamps),
                "trade_count": [10] * len(timestamps),
                "vwap": [100.25 + value for value in values],
            },
            index=index,
        )

    def test_rolling_window_is_ten_calendar_years(self):
        start, end = rolling_window(
            datetime(2026, 7, 20, 20, 0, tzinfo=timezone.utc)
        )
        self.assertEqual(start, datetime(2016, 7, 20, 0, 0, tzinfo=timezone.utc))
        self.assertEqual(end, datetime(2026, 7, 20, 20, 0, tzinfo=timezone.utc))

    def test_output_paths_only_include_requested_intervals(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            roots = {interval: root / interval for interval in OUTPUT_INTERVALS}
            paths = output_paths("BRK/B", "raw", "parquet", roots)
        self.assertEqual(tuple(paths), OUTPUT_INTERVALS)
        self.assertEqual(paths["1hour"].name, "BRK-B_1hour_sip_historical.parquet")

    def test_thirty_minute_source_builds_only_regular_long_term_outputs(self):
        outputs = build_output_bars(self.make_thirty_minute_frame())
        self.assertEqual(tuple(outputs), OUTPUT_INTERVALS)
        self.assertEqual(len(outputs["1hour"]), 7)
        self.assertEqual(outputs["1hour"]["source_minutes"].tolist(), [60] * 6 + [30])
        self.assertEqual(len(outputs["4hour"]), 2)
        self.assertEqual(outputs["4hour"]["source_minutes"].tolist(), [240, 150])
        self.assertEqual(len(outputs["1day"]), 1)
        self.assertEqual(outputs["1day"].iloc[0]["source_minutes"], 390)
        first_timestamp = outputs["1hour"].index.get_level_values("timestamp")[0]
        self.assertEqual(first_timestamp, pd.Timestamp("2025-02-03 14:30:00+00:00"))

    @patch("data_collection.collect_sip_long_term.time.sleep")
    def test_fetch_chunk_requests_thirty_minute_sip_bars(self, _mock_sleep):
        client = Mock()
        client.get_stock_bars.return_value.df = pd.DataFrame()
        fetch_chunk(
            client,
            "BRK/B",
            datetime(2025, 1, 1, tzinfo=timezone.utc),
            datetime(2025, 1, 2, tzinfo=timezone.utc),
            "adjusted",
        )
        request = client.get_stock_bars.call_args.args[0]
        self.assertEqual(request.symbol_or_symbols, ["BRK.B"])
        self.assertEqual(request.timeframe.amount, BASE_TIMEFRAME_MINUTES)


if __name__ == "__main__":
    unittest.main()
