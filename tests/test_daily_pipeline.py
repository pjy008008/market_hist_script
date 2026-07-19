import os
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd

from data_collection.collect_sip_1min import CollectionResult
from daily_pipeline import (
    choose_storage_format,
    completed_collection_window,
    load_pipeline_symbols,
    parse_args,
    run_collection,
    run_pipeline,
)
from pipeline_state import PipelineStateStore


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
        refresh_start = datetime(2025, 6, 20, 13, 30, tzinfo=timezone.utc)
        changes = {"AAPL": pd.Timestamp("2025-07-03 13:30:00+00:00")}
        with (
            patch.dict(
                os.environ,
                {"ALPACA_API_KEY": "test-key", "ALPACA_SECRET_KEY": "test-secret"},
            ),
            patch("daily_pipeline.load_dotenv"),
            patch("daily_pipeline.completed_collection_window", return_value=window),
            patch("daily_pipeline.adjusted_refresh_start", return_value=refresh_start),
            patch("daily_pipeline.load_pipeline_symbols", return_value=["AAPL"]),
            patch("daily_pipeline.PipelineStateStore") as mock_state_class,
            patch("daily_pipeline.StockHistoricalDataClient") as mock_client,
            patch(
                "daily_pipeline.run_collection", return_value=([], changes)
            ) as mock_collect,
            patch(
                "daily_pipeline.run_quality_control",
                return_value=(["AAPL"], [], changes, 0),
            ) as mock_quality,
            patch(
                "daily_pipeline.run_filter",
                return_value=(1, 100, 80, []),
            ) as mock_filter,
            patch(
                "daily_pipeline.run_resample",
                return_value=(1, 80, 16, []),
            ) as mock_resample,
            patch("daily_pipeline.run_validation", return_value=(1, 0)) as mock_audit,
            patch("daily_pipeline.save_failure_report") as mock_failure_report,
        ):
            result = run_pipeline("parquet")

        self.assertEqual(result, 0)
        state = mock_state_class.return_value
        mock_collect.assert_called_once_with(
            mock_client.return_value,
            ["AAPL"],
            "parquet",
            window[0],
            window[1],
            state,
            pd.Timestamp(window[1]).isoformat(),
            refresh_start,
        )
        mock_quality.assert_called_once_with(
            mock_client.return_value,
            "parquet",
            ["AAPL"],
            state,
            pd.Timestamp(window[1]).isoformat(),
            window[0],
            window[1],
            refresh_start,
            changes,
        )
        mock_filter.assert_called_once_with(
            "parquet",
            ["AAPL"],
            state,
            pd.Timestamp(window[1]).isoformat(),
            window[0],
            window[1],
            changes,
        )
        mock_resample.assert_called_once_with(
            "parquet",
            ["AAPL"],
            state,
            pd.Timestamp(window[1]).isoformat(),
            window[0],
            window[1],
            changes,
        )
        mock_audit.assert_called_once_with("parquet")
        mock_failure_report.assert_called_once_with(
            [], "parquet", pd.Timestamp(window[1]).isoformat()
        )
        state.finish_run.assert_called_once_with("success", [])

    @patch(
        "daily_pipeline.update_symbol_data",
        side_effect=[CollectionResult(False), CollectionResult(True)],
    )
    def test_collection_retries_only_failed_symbol(self, mock_process):
        with tempfile.TemporaryDirectory() as temporary_directory:
            state = PipelineStateStore(Path(temporary_directory) / "state.json")
            failures, changes = run_collection(
                Mock(),
                ["AAPL"],
                "csv",
                datetime(2022, 1, 1, tzinfo=timezone.utc),
                datetime(2025, 1, 1, tzinfo=timezone.utc),
                state,
                "2025-01-01T00:00:00+00:00",
                datetime(2024, 12, 15, tzinfo=timezone.utc),
                retry_delay=0,
            )

            self.assertEqual(failures, [])
            self.assertEqual(changes, {})
            self.assertEqual(mock_process.call_count, 2)
            checkpoint = state.data["checkpoints"]["csv"]["AAPL"]["collection"]
            self.assertEqual(checkpoint["status"], "success")
            self.assertEqual(checkpoint["attempt"], 2)


if __name__ == "__main__":
    unittest.main()
