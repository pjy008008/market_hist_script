import os
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, call, patch

import pandas as pd

from data_collection.collect_sip_1min import CollectionResult
from daily_pipeline import (
    choose_storage_format,
    completed_collection_window,
    load_pipeline_symbols,
    parse_args,
    run_collection,
    run_filter,
    run_pipeline,
    run_resample,
    run_validation,
)
from pipeline_reporting import DailyReportStore
from pipeline_state import PipelineStateStore


class DailyPipelineTests(unittest.TestCase):
    @patch("builtins.input", return_value="2")
    def test_only_interactive_choice_selects_parquet(self, _mock_input):
        self.assertEqual(choose_storage_format(), "parquet")

    def test_cli_defaults_to_fast_quality(self):
        args = parse_args(["--format", "csv"])
        self.assertEqual(args.storage_format, "csv")
        self.assertFalse(args.deep_quality)

    def test_cli_can_enable_deep_quality(self):
        args = parse_args(["--format", "parquet", "--deep-quality"])
        self.assertTrue(args.deep_quality)

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
            patch("daily_pipeline.DailyReportStore") as mock_report_store_class,
            patch("daily_pipeline.StockHistoricalDataClient") as mock_client,
            patch("daily_pipeline.TradingClient") as mock_asset_client,
            patch("daily_pipeline.InactiveSymbolCache") as mock_inactive_cache,
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
            patch("daily_pipeline.save_run_summary") as mock_run_summary,
        ):
            result = run_pipeline("parquet")

        self.assertEqual(result, 0)
        state = mock_state_class.return_value
        reports = mock_report_store_class.for_target_session.return_value
        reports.prune_history.assert_called_once_with()
        mock_collect.assert_has_calls(
            [
                call(
                    mock_client.return_value,
                    ["AAPL"],
                    "parquet",
                    window[0],
                    window[1],
                    state,
                    pd.Timestamp(window[1]).isoformat(),
                    refresh_start,
                    data_type="adjusted",
                    asset_client=mock_asset_client.return_value,
                    inactive_cache=mock_inactive_cache.return_value,
                ),
                call(
                    mock_client.return_value,
                    ["AAPL"],
                    "parquet",
                    window[0],
                    window[1],
                    state,
                    pd.Timestamp(window[1]).isoformat(),
                    None,
                    data_type="raw",
                    asset_client=mock_asset_client.return_value,
                    inactive_cache=mock_inactive_cache.return_value,
                ),
            ]
        )
        self.assertEqual(mock_collect.call_count, 2)
        mock_quality.assert_has_calls(
            [
                call(
                    mock_client.return_value,
                    "parquet",
                    ["AAPL"],
                    state,
                    pd.Timestamp(window[1]).isoformat(),
                    window[0],
                    window[1],
                    refresh_start,
                    changes,
                    False,
                    reports,
                    data_type="adjusted",
                ),
                call(
                    mock_client.return_value,
                    "parquet",
                    ["AAPL"],
                    state,
                    pd.Timestamp(window[1]).isoformat(),
                    window[0],
                    window[1],
                    refresh_start,
                    changes,
                    False,
                    reports,
                    data_type="raw",
                ),
            ]
        )
        self.assertEqual(mock_quality.call_count, 2)
        mock_filter.assert_has_calls(
            [
                call(
                    "parquet",
                    ["AAPL"],
                    state,
                    pd.Timestamp(window[1]).isoformat(),
                    window[0],
                    window[1],
                    changes,
                    data_type="adjusted",
                ),
                call(
                    "parquet",
                    ["AAPL"],
                    state,
                    pd.Timestamp(window[1]).isoformat(),
                    window[0],
                    window[1],
                    changes,
                    data_type="raw",
                ),
            ]
        )
        self.assertEqual(mock_filter.call_count, 2)
        intervals = ("5min", "15min", "1hour", "4hour", "1day")
        mock_resample.assert_has_calls(
            [
                call(
                    "parquet",
                    ["AAPL"],
                    state,
                    pd.Timestamp(window[1]).isoformat(),
                    window[0],
                    window[1],
                    changes,
                    data_type=data_type,
                    bar_interval=interval,
                )
                for data_type in ("adjusted", "raw")
                for interval in intervals
            ]
        )
        self.assertEqual(mock_resample.call_count, 10)
        mock_audit.assert_has_calls(
            [
                call(
                    "parquet",
                    detailed=False,
                    reports=reports,
                    data_type="adjusted",
                ),
                call(
                    "parquet",
                    detailed=False,
                    reports=reports,
                    data_type="raw",
                ),
            ]
        )
        self.assertEqual(mock_audit.call_count, 2)
        mock_failure_report.assert_called_once_with(
            [],
            "parquet",
            pd.Timestamp(window[1]).isoformat(),
            reports,
        )
        mock_run_summary.assert_called_once()
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

    @patch(
        "daily_pipeline.update_symbol_data",
        return_value=CollectionResult(True),
    )
    def test_raw_collection_uses_independent_checkpoint(self, mock_process):
        with tempfile.TemporaryDirectory() as temporary_directory:
            state = PipelineStateStore(Path(temporary_directory) / "state.json")
            failures, _ = run_collection(
                Mock(),
                ["AAPL"],
                "parquet",
                datetime(2022, 1, 1, tzinfo=timezone.utc),
                datetime(2025, 1, 1, tzinfo=timezone.utc),
                state,
                "2025-01-01T00:00:00+00:00",
                None,
                retry_delay=0,
                data_type="raw",
            )

            self.assertEqual(failures, [])
            self.assertIn(
                "raw_collection",
                state.data["checkpoints"]["parquet"]["AAPL"],
            )
            self.assertEqual(mock_process.call_args.args[2], "raw")

    def test_raw_postprocessing_uses_separate_paths_and_reports(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            collection_root = root / "sip_market_data"
            filtered_root = root / "regular_sip_1min_market_data"
            resampled_roots = {
                interval: root / f"regular_sip_{interval}_market_data"
                for interval in ("5min", "15min", "1hour", "4hour", "1day")
            }
            input_path = (
                collection_root
                / "raw"
                / "csv"
                / "AAPL_1min_sip_historical.csv"
            )
            input_path.parent.mkdir(parents=True)
            timestamps = pd.date_range(
                "2025-02-03 14:30:00+00:00",
                periods=5,
                freq="1min",
            )
            index = pd.MultiIndex.from_arrays(
                [["AAPL"] * 5, timestamps],
                names=["symbol", "timestamp"],
            )
            pd.DataFrame(
                {
                    "open": [10, 11, 12, 13, 14],
                    "high": [11, 12, 13, 14, 15],
                    "low": [9, 10, 11, 12, 13],
                    "close": [10.5, 11.5, 12.5, 13.5, 14.5],
                    "volume": [100, 200, 300, 400, 500],
                },
                index=index,
            ).to_csv(input_path, index=True)

            state = PipelineStateStore(root / "state.json")
            target = "2025-02-03T21:00:00+00:00"
            start = datetime(2025, 2, 3, tzinfo=timezone.utc)
            end = datetime(2025, 2, 4, tzinfo=timezone.utc)
            reports = DailyReportStore.for_target_session(
                target,
                "csv",
                "XNYS",
                report_root=root / "report",
            )
            with (
                patch("daily_pipeline.COLLECTION_ROOT", collection_root),
                patch("daily_pipeline.FILTERED_ROOT", filtered_root),
                patch("daily_pipeline.RESAMPLED_ROOTS", resampled_roots),
            ):
                filtered = run_filter(
                    "csv",
                    ["AAPL"],
                    state,
                    target,
                    start,
                    end,
                    data_type="raw",
                )
                resampled = {
                    interval: run_resample(
                        "csv",
                        ["AAPL"],
                        state,
                        target,
                        start,
                        end,
                        data_type="raw",
                        bar_interval=interval,
                    )
                    for interval in resampled_roots
                }
                audited = run_validation(
                    "csv",
                    reports=reports,
                    data_type="raw",
                )

            self.assertEqual(filtered[:3], (1, 5, 5))
            self.assertTrue(
                all(result[:3] == (1, 5, 1) for result in resampled.values())
            )
            self.assertEqual(audited, (6, 0))
            self.assertTrue(
                (
                    filtered_root
                    / "raw"
                    / "csv"
                    / "AAPL_1min_sip_historical.csv"
                ).is_file()
            )
            for interval, resampled_root in resampled_roots.items():
                self.assertTrue(
                    (
                        resampled_root
                        / "raw"
                        / "csv"
                        / f"AAPL_{interval}_sip_historical.csv"
                    ).is_file()
                )
            self.assertTrue(
                (reports.latest_root / "raw_1min_summary.csv").is_file()
            )
            for interval in resampled_roots:
                self.assertTrue(
                    (reports.latest_root / f"raw_{interval}_summary.csv").is_file()
                )


if __name__ == "__main__":
    unittest.main()
