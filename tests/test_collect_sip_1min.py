import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd
from alpaca.trading.enums import AssetStatus

from data_collection.collect_sip_1min import (
    alpaca_symbol,
    collection_window,
    earliest_changed_timestamp,
    fetch_chunk,
    InactiveSymbolCache,
    merge_frames,
    save_local_data,
    should_skip_inactive_symbol,
    storage_path,
    update_symbol_data,
)


class CollectSipOneMinuteTests(unittest.TestCase):
    @staticmethod
    def make_frame(timestamps: list[str], values: list[int]) -> pd.DataFrame:
        index = pd.MultiIndex.from_arrays(
            [
                ["AAPL"] * len(timestamps),
                pd.to_datetime(timestamps, utc=True),
            ],
            names=["symbol", "timestamp"],
        )
        return pd.DataFrame({"close": values}, index=index)

    def test_collection_window_uses_exact_three_years_and_fifteen_minute_delay(self):
        now = datetime(2026, 7, 19, 12, 0, tzinfo=timezone.utc)
        start, end = collection_window(now)

        self.assertEqual(end, datetime(2026, 7, 19, 11, 45, tzinfo=timezone.utc))
        self.assertEqual(start, datetime(2023, 7, 19, 12, 0, tzinfo=timezone.utc))

    def test_storage_path_separates_feed_type_and_format(self):
        path = storage_path("BRK/B", "adjusted", "parquet", Path("sip_market_data"))
        self.assertEqual(
            path,
            Path("sip_market_data/adjusted/parquet/BRK-B_1min_sip_historical.parquet"),
        )

    def test_alpaca_symbol_uses_dot_for_class_shares(self):
        self.assertEqual(alpaca_symbol("BRK/B"), "BRK.B")
        self.assertEqual(alpaca_symbol("BF/B"), "BF.B")
        self.assertEqual(alpaca_symbol("AAPL"), "AAPL")

    def test_fetch_chunk_sends_alpaca_class_share_symbol(self):
        client = Mock()
        client.get_stock_bars.return_value = Mock(df=pd.DataFrame())

        result = fetch_chunk(
            client,
            "BRK/B",
            datetime(2025, 1, 1, tzinfo=timezone.utc),
            datetime(2025, 1, 2, tzinfo=timezone.utc),
            "adjusted",
        )

        request = client.get_stock_bars.call_args.args[0]
        self.assertEqual(request.symbol_or_symbols, ["BRK.B"])
        self.assertTrue(result.empty)

    def test_merge_deduplicates_and_keeps_latest_value_inside_window(self):
        existing = self.make_frame(
            ["2025-01-02 14:30:00Z", "2025-01-02 14:31:00Z"],
            [100, 101],
        )
        new = self.make_frame(
            ["2025-01-02 14:31:00Z", "2025-01-02 14:32:00Z"],
            [201, 202],
        )

        combined = merge_frames(
            existing,
            [new],
            datetime(2025, 1, 2, 14, 30, tzinfo=timezone.utc),
            datetime(2025, 1, 2, 14, 33, tzinfo=timezone.utc),
        )

        self.assertEqual(len(combined), 3)
        self.assertEqual(combined.loc[("AAPL", pd.Timestamp("2025-01-02 14:31:00Z")), "close"], 201)

    def test_detects_first_adjusted_overlap_revision(self):
        existing = self.make_frame(
            ["2025-01-02 14:30:00Z", "2025-01-02 14:31:00Z"],
            [100, 101],
        )
        refreshed = self.make_frame(
            ["2025-01-02 14:30:00Z", "2025-01-02 14:31:00Z"],
            [50, 50],
        )

        changed = earliest_changed_timestamp(existing, refreshed)

        self.assertEqual(changed, pd.Timestamp("2025-01-02 14:30:00Z"))

    def test_comparison_deduplicates_overlapping_chunk_boundaries(self):
        existing = self.make_frame(
            ["2025-01-02 14:30:00Z", "2025-01-02 14:31:00Z"],
            [100, 101],
        )
        refreshed = self.make_frame(
            [
                "2025-01-02 14:30:00Z",
                "2025-01-02 14:31:00Z",
                "2025-01-02 14:31:00Z",
            ],
            [100, 101, 101],
        )

        changed = earliest_changed_timestamp(existing, refreshed)

        self.assertIsNone(changed)

    def test_update_saves_when_fetched_chunks_share_a_boundary_bar(self):
        existing = self.make_frame(
            ["2025-01-02 14:30:00Z", "2025-01-02 14:31:00Z"],
            [100, 101],
        )
        first_chunk = self.make_frame(
            ["2025-01-02 14:30:00Z", "2025-01-02 14:31:00Z"],
            [100, 101],
        )
        second_chunk = self.make_frame(
            ["2025-01-02 14:31:00Z", "2025-01-02 14:32:00Z"],
            [101, 102],
        )
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            path = storage_path("AAPL", "adjusted", "csv", root)
            save_local_data(existing, path, "csv")
            with patch(
                "data_collection.collect_sip_1min.fetch_range",
                return_value=[first_chunk, second_chunk],
            ):
                result = update_symbol_data(
                    Mock(),
                    "AAPL",
                    "adjusted",
                    "csv",
                    root,
                    datetime(2025, 1, 1, tzinfo=timezone.utc),
                    datetime(2025, 1, 2, 14, 33, tzinfo=timezone.utc),
                    7,
                    0,
                    datetime(2025, 1, 2, 14, 30, tzinfo=timezone.utc),
                )

            saved = pd.read_csv(path)

        self.assertTrue(result.success)
        self.assertFalse(result.adjustment_revision)
        self.assertEqual(result.added_rows, 1)
        self.assertEqual(len(saved), 3)

    def test_adjusted_revision_refreshes_full_rolling_window(self):
        existing = self.make_frame(
            ["2025-01-02 14:30:00Z", "2025-01-02 14:31:00Z"],
            [100, 101],
        )
        refreshed = self.make_frame(
            ["2025-01-02 14:30:00Z", "2025-01-02 14:31:00Z"],
            [50, 50],
        )
        historical = self.make_frame(["2025-01-01 14:30:00Z"], [49])
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            path = storage_path("AAPL", "adjusted", "csv", root)
            save_local_data(existing, path, "csv")
            with patch(
                "data_collection.collect_sip_1min.fetch_range",
                side_effect=[[refreshed], [historical]],
            ) as mock_fetch:
                result = update_symbol_data(
                    Mock(),
                    "AAPL",
                    "adjusted",
                    "csv",
                    root,
                    datetime(2025, 1, 1, tzinfo=timezone.utc),
                    datetime(2025, 1, 2, 14, 32, tzinfo=timezone.utc),
                    7,
                    0,
                    datetime(2025, 1, 2, 14, 30, tzinfo=timezone.utc),
                )

        self.assertTrue(result.success)
        self.assertTrue(result.adjustment_revision)
        self.assertEqual(mock_fetch.call_count, 2)
        self.assertEqual(
            result.changed_from_utc,
            pd.Timestamp("2025-01-01 14:30:00+00:00"),
        )

    def test_confirmed_inactive_symbol_is_cached_and_skipped(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            cache_path = Path(temporary_directory) / "inactive_symbols.json"
            cache = InactiveSymbolCache(cache_path)
            asset_client = Mock()
            asset_client.get_asset.return_value = Mock(status=AssetStatus.INACTIVE)
            last_bar = datetime(2023, 10, 12, 23, 48, tzinfo=timezone.utc)
            end_time = datetime(2026, 7, 20, tzinfo=timezone.utc)

            first = should_skip_inactive_symbol(
                asset_client, cache, "ATVI", last_bar, end_time
            )
            reloaded_cache = InactiveSymbolCache(cache_path)
            cached_client = Mock()
            second = should_skip_inactive_symbol(
                cached_client, reloaded_cache, "ATVI", last_bar, end_time
            )

        self.assertTrue(first)
        self.assertTrue(second)
        asset_client.get_asset.assert_called_once_with("ATVI")
        cached_client.get_asset.assert_not_called()

    def test_active_or_recent_symbol_is_not_skipped(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            cache = InactiveSymbolCache(
                Path(temporary_directory) / "inactive_symbols.json"
            )
            asset_client = Mock()
            asset_client.get_asset.return_value = Mock(status=AssetStatus.ACTIVE)
            end_time = datetime(2026, 7, 20, tzinfo=timezone.utc)

            active = should_skip_inactive_symbol(
                asset_client,
                cache,
                "AAPL",
                datetime(2026, 5, 1, tzinfo=timezone.utc),
                end_time,
            )
            active_again = should_skip_inactive_symbol(
                asset_client,
                cache,
                "AAPL",
                datetime(2026, 5, 1, tzinfo=timezone.utc),
                end_time,
            )
            recent = should_skip_inactive_symbol(
                asset_client,
                cache,
                "MSFT",
                datetime(2026, 7, 19, tzinfo=timezone.utc),
                end_time,
            )

        self.assertFalse(active)
        self.assertFalse(active_again)
        self.assertFalse(recent)
        asset_client.get_asset.assert_called_once_with("AAPL")

    def test_update_does_not_fetch_bars_for_confirmed_inactive_symbol(self):
        existing = self.make_frame(["2023-10-12 23:48:00Z"], [95])
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            path = storage_path("ATVI", "adjusted", "csv", root)
            save_local_data(existing, path, "csv")
            cache = InactiveSymbolCache(root / "inactive_symbols.json")
            asset_client = Mock()
            asset_client.get_asset.return_value = Mock(status=AssetStatus.INACTIVE)
            with patch("data_collection.collect_sip_1min.fetch_range") as mock_fetch:
                result = update_symbol_data(
                    Mock(),
                    "ATVI",
                    "adjusted",
                    "csv",
                    root,
                    datetime(2023, 7, 20, tzinfo=timezone.utc),
                    datetime(2026, 7, 20, tzinfo=timezone.utc),
                    7,
                    0,
                    datetime(2026, 7, 1, tzinfo=timezone.utc),
                    asset_client,
                    cache,
                )

        self.assertTrue(result.success)
        self.assertTrue(result.inactive)
        self.assertEqual(result.added_rows, 0)
        mock_fetch.assert_not_called()


if __name__ == "__main__":
    unittest.main()
