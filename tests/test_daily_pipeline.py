import os
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from daily_pipeline import (
    choose_storage_format,
    completed_collection_window,
    load_pipeline_symbols,
    parse_args,
    run_pipeline,
)


class DailyPipelineTests(unittest.TestCase):
    @patch("builtins.input", return_value="2")
    def test_only_interactive_choice_selects_parquet(self, _mock_input):
        self.assertEqual(choose_storage_format(), "parquet")

    def test_cli_only_exposes_storage_format(self):
        args = parse_args(["--format", "csv"])
        self.assertEqual(args.storage_format, "csv")

    def test_early_close_is_used_only_after_data_delay(self):
        before_delay = datetime(2025, 7, 3, 17, 14, tzinfo=timezone.utc)
        _, end_before = completed_collection_window(before_delay)
        self.assertEqual(
            pd.Timestamp(end_before),
            pd.Timestamp("2025-07-02 20:00:00+00:00"),
        )

        after_delay = datetime(2025, 7, 3, 17, 16, tzinfo=timezone.utc)
        start_after, end_after = completed_collection_window(after_delay)
        self.assertEqual(
            pd.Timestamp(end_after),
            pd.Timestamp("2025-07-03 17:00:00+00:00"),
        )
        self.assertEqual(
            pd.Timestamp(start_after),
            pd.Timestamp("2022-07-03 17:00:00+00:00"),
        )

    @patch(
        "daily_pipeline.get_historical_sp500_tickers",
        return_value=["AAPL", "MSFT"],
    )
    def test_valid_ticker_refresh_replaces_cache(self, _mock_fetch):
        with tempfile.TemporaryDirectory() as temporary_directory:
            ticker_file = Path(temporary_directory) / "tickers.txt"
            symbols = load_pipeline_symbols(ticker_file, minimum_count=2)

            self.assertEqual(symbols, ["AAPL", "MSFT"])
            self.assertEqual(
                ticker_file.read_text(encoding="utf-8"),
                "AAPL\nMSFT\n",
            )

    @patch(
        "daily_pipeline.get_historical_sp500_tickers",
        return_value=["AAPL"],
    )
    def test_invalid_ticker_refresh_preserves_valid_cache(self, _mock_fetch):
        with tempfile.TemporaryDirectory() as temporary_directory:
            ticker_file = Path(temporary_directory) / "tickers.txt"
            ticker_file.write_text("AAPL\nMSFT\n", encoding="utf-8")

            symbols = load_pipeline_symbols(ticker_file, minimum_count=2)

            self.assertEqual(symbols, ["AAPL", "MSFT"])
            self.assertEqual(
                ticker_file.read_text(encoding="utf-8"),
                "AAPL\nMSFT\n",
            )

    def test_pipeline_runs_collection_filter_and_validation_in_order(self):
        window = (
            datetime(2022, 7, 3, 17, 0, tzinfo=timezone.utc),
            datetime(2025, 7, 3, 17, 0, tzinfo=timezone.utc),
        )
        with (
            patch.dict(
                os.environ,
                {"ALPACA_API_KEY": "test-key", "ALPACA_SECRET_KEY": "test-secret"},
            ),
            patch("daily_pipeline.load_dotenv"),
            patch("daily_pipeline.completed_collection_window", return_value=window),
            patch("daily_pipeline.load_pipeline_symbols", return_value=["AAPL"]),
            patch("daily_pipeline.StockHistoricalDataClient") as mock_client,
            patch("daily_pipeline.run_collection", return_value=[]) as mock_collect,
            patch("daily_pipeline.run_filter", return_value=(1, 100, 80)) as mock_filter,
            patch("daily_pipeline.run_validation", return_value=(1, 0)) as mock_audit,
        ):
            result = run_pipeline("parquet")

        self.assertEqual(result, 0)
        mock_collect.assert_called_once_with(
            mock_client.return_value,
            ["AAPL"],
            "parquet",
            window[0],
            window[1],
        )
        mock_filter.assert_called_once_with("parquet")
        mock_audit.assert_called_once_with("parquet")


if __name__ == "__main__":
    unittest.main()
