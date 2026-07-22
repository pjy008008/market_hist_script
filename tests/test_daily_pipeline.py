import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd

from data_collection.collect_sip_1min import CollectionResult
from daily_pipeline import (
    OUTPUT_INTERVALS,
    _checkpoint_complete,
    choose_storage_format,
    completed_collection_window,
    load_pipeline_symbols,
    parse_args,
    run_collection,
)
from pipeline_state import PipelineStateStore


class DailyPipelineTests(unittest.TestCase):
    @patch("builtins.input", return_value="2")
    def test_only_interactive_choice_selects_parquet(self, _mock_input):
        self.assertEqual(choose_storage_format(), "parquet")

    def test_cli_defaults_to_summary_quality(self):
        args = parse_args(["--format", "csv"])
        self.assertEqual(args.storage_format, "csv")
        self.assertFalse(args.deep_quality)

    def test_cli_can_enable_missing_interval_reports(self):
        args = parse_args(["--format", "parquet", "--deep-quality"])
        self.assertTrue(args.deep_quality)

    def test_window_uses_ten_years_and_last_completed_session(self):
        before_close = datetime(2025, 7, 3, 16, 30, tzinfo=timezone.utc)
        start, end = completed_collection_window(before_close)
        self.assertEqual(end, datetime(2025, 7, 2, 20, 0, tzinfo=timezone.utc))
        self.assertEqual(start, datetime(2015, 7, 2, 0, 0, tzinfo=timezone.utc))

        after_early_close_delay = datetime(2025, 7, 3, 17, 20, tzinfo=timezone.utc)
        start, end = completed_collection_window(after_early_close_delay)
        self.assertEqual(end, datetime(2025, 7, 3, 17, 0, tzinfo=timezone.utc))
        self.assertEqual(start, datetime(2015, 7, 3, 0, 0, tzinfo=timezone.utc))

    @patch("daily_pipeline.get_historical_sp500_tickers")
    def test_ticker_policy_uses_ten_year_membership(self, mock_fetch):
        mock_fetch.return_value = ["MSFT", "AAPL"]
        with tempfile.TemporaryDirectory() as temporary_directory:
            ticker_file = Path(temporary_directory) / "tickers.txt"
            symbols = load_pipeline_symbols(
                ticker_file,
                minimum_count=2,
                additional_symbols=["SPY"],
            )
        mock_fetch.assert_called_once_with(years=10)
        self.assertEqual(symbols, ["AAPL", "MSFT", "SPY"])

    def test_checkpoint_requires_all_three_output_files(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            state = PipelineStateStore(root / "state.json")
            fake_paths = {
                interval: root / f"AAPL_{interval}.parquet"
                for interval in OUTPUT_INTERVALS
            }
            for path in fake_paths.values():
                path.write_text("ok", encoding="utf-8")
            state.mark_stage(
                "parquet",
                "AAPL",
                "long_term_collection",
                "success",
                "2025-01-02T21:00:00+00:00",
                1,
            )
            with patch("daily_pipeline.output_paths", return_value=fake_paths):
                self.assertTrue(
                    _checkpoint_complete(
                        state,
                        "parquet",
                        "AAPL",
                        "adjusted",
                        "2025-01-02T21:00:00+00:00",
                    )
                )
                fake_paths["4hour"].unlink()
                self.assertFalse(
                    _checkpoint_complete(
                        state,
                        "parquet",
                        "AAPL",
                        "adjusted",
                        "2025-01-02T21:00:00+00:00",
                    )
                )

    @patch("daily_pipeline.update_symbol_data")
    @patch("daily_pipeline._checkpoint_complete", return_value=False)
    def test_collection_retries_only_failed_symbol(self, _mock_complete, mock_update):
        mock_update.side_effect = [
            CollectionResult(True, added_rows=3),
            CollectionResult(False),
            CollectionResult(True, added_rows=2),
        ]
        state = Mock()
        failures, added_rows = run_collection(
            Mock(),
            ["AAPL", "MSFT"],
            "parquet",
            datetime(2016, 1, 1, tzinfo=timezone.utc),
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            state,
            "2026-01-01T21:00:00+00:00",
            None,
            "raw",
            Mock(),
            Mock(),
            retry_delay=0,
        )
        self.assertEqual(failures, [])
        self.assertEqual(added_rows, 5)
        self.assertEqual(mock_update.call_count, 3)
        self.assertEqual(mock_update.call_args_list[2].args[1], "MSFT")


if __name__ == "__main__":
    unittest.main()
