import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from data_filtering.filter_regular_session import (
    build_sources,
    choose_dataset,
    choose_data_type,
    choose_storage_format,
    filter_regular_session,
    load_market_data,
    parse_timestamp_values,
    process_source,
    parse_args,
)


class FilterRegularSessionTests(unittest.TestCase):
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

    def kept_timestamps(self, timestamps: list[str]) -> list[pd.Timestamp]:
        filtered = filter_regular_session(self.make_frame(timestamps))
        return list(filtered.index.get_level_values("timestamp"))

    def test_standard_session_excludes_pre_market_and_exact_close(self):
        timestamps = [
            "2025-02-03 14:25:00+00:00",
            "2025-02-03 14:30:00+00:00",
            "2025-02-03 20:55:00+00:00",
            "2025-02-03 21:00:00+00:00",
        ]
        self.assertEqual(
            self.kept_timestamps(timestamps),
            list(pd.to_datetime(timestamps[1:3], utc=True)),
        )

    def test_daylight_saving_time_changes_utc_session(self):
        timestamps = [
            "2025-03-07 14:30:00+00:00",
            "2025-03-10 13:25:00+00:00",
            "2025-03-10 13:30:00+00:00",
        ]
        self.assertEqual(
            self.kept_timestamps(timestamps),
            [pd.Timestamp(timestamps[0]), pd.Timestamp(timestamps[2])],
        )

    def test_holiday_and_early_close_follow_exchange_schedule(self):
        timestamps = [
            "2025-07-04 15:00:00+00:00",
            "2025-07-03 16:55:00+00:00",
            "2025-07-03 17:00:00+00:00",
            "2025-07-03 18:00:00+00:00",
        ]
        self.assertEqual(
            self.kept_timestamps(timestamps),
            [pd.Timestamp(timestamps[1])],
        )

    def test_one_minute_sip_bar_keeps_last_regular_minute_only(self):
        timestamps = [
            "2025-02-03 20:59:00+00:00",
            "2025-02-03 21:00:00+00:00",
        ]
        self.assertEqual(
            self.kept_timestamps(timestamps),
            [pd.Timestamp(timestamps[0])],
        )

    def test_sample_sip_epoch_milliseconds_are_parsed_and_filtered(self):
        timestamps = [
            1689773340000,  # 2023-07-19 09:29 America/New_York
            1689773400000,  # 2023-07-19 09:30 America/New_York
            1689773460000,  # 2023-07-19 09:31 America/New_York
        ]
        index = pd.MultiIndex.from_arrays(
            [["AAP"] * len(timestamps), timestamps],
            names=["symbol", "timestamp"],
        )
        frame = pd.DataFrame({"close": [65.89, 66.09, 66.40]}, index=index)

        filtered = filter_regular_session(frame)

        self.assertEqual(
            list(filtered.index.get_level_values("timestamp")),
            list(pd.to_datetime(timestamps[1:], unit="ms", utc=True)),
        )

    def test_numeric_unix_timestamp_units_are_auto_detected(self):
        expected = pd.Timestamp("2023-07-18 16:21:00+00:00")
        values_by_unit = {
            "seconds": [1689697260],
            "milliseconds": [1689697260000],
            "microseconds": [1689697260000000],
            "nanoseconds": [1689697260000000000],
        }
        for label, values in values_by_unit.items():
            with self.subTest(unit=label):
                self.assertEqual(parse_timestamp_values(values)[0], expected)

    def test_csv_epoch_milliseconds_are_normalized_on_load(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            input_path = Path(temporary_directory) / "AAP.csv"
            pd.DataFrame(
                {
                    "symbol": ["AAP"],
                    "timestamp": [1689697260000],
                    "close": [65.30],
                }
            ).to_csv(input_path, index=False)

            result = load_market_data(input_path, "csv")

            self.assertEqual(
                result.index.get_level_values("timestamp")[0],
                pd.Timestamp("2023-07-18 16:21:00+00:00"),
            )


class InteractiveSelectionTests(unittest.TestCase):
    @patch("builtins.input", return_value="2")
    def test_dataset_second_choice_selects_sip(self, _mock_input):
        self.assertEqual(choose_dataset(), "sip")

    @patch("builtins.input", return_value="2")
    def test_data_type_second_choice_selects_adjusted(self, _mock_input):
        self.assertEqual(choose_data_type(), "adjusted")

    @patch("builtins.input", side_effect=["invalid", "2"])
    def test_format_reprompts_and_selects_parquet(self, _mock_input):
        self.assertEqual(choose_storage_format(), "parquet")

    def test_omitted_cli_selections_remain_unset_for_interactive_prompt(self):
        args = parse_args([])
        self.assertIsNone(args.data_type)
        self.assertIsNone(args.storage_format)
        self.assertIsNone(args.dataset)
        self.assertIsNone(args.output_dir)

    def test_sip_source_and_destination_are_separate(self):
        sources = build_sources(
            Path("project"),
            Path("project/regular_sip_market_data"),
            "adjusted",
            "parquet",
            "sip",
        )
        self.assertEqual(
            sources[0].source_dir,
            Path("project/sip_market_data/adjusted/parquet"),
        )
        self.assertEqual(
            sources[0].destination_dir,
            Path("project/regular_sip_market_data/adjusted/parquet"),
        )

    def test_sip_csv_is_filtered_and_saved_end_to_end(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            project_root = Path(temporary_directory)
            source = build_sources(
                project_root,
                project_root / "regular_sip_market_data",
                "raw",
                "csv",
                "sip",
            )[0]
            source.source_dir.mkdir(parents=True)
            input_path = source.source_dir / "AAPL_1min_sip_historical.csv"
            frame = FilterRegularSessionTests.make_frame(
                [
                    "2025-02-03 14:29:00+00:00",
                    "2025-02-03 14:30:00+00:00",
                    "2025-02-03 20:59:00+00:00",
                    "2025-02-03 21:00:00+00:00",
                ]
            )
            frame.to_csv(input_path, index=True)

            self.assertEqual(process_source(source, "XNYS"), (1, 4, 2))
            output_path = source.destination_dir / input_path.name
            result = pd.read_csv(output_path)
            self.assertEqual(len(result), 2)
            self.assertEqual(
                pd.to_datetime(result["timestamp"], utc=True).tolist(),
                pd.to_datetime(
                    [
                        "2025-02-03 14:30:00+00:00",
                        "2025-02-03 20:59:00+00:00",
                    ],
                    utc=True,
                ).tolist(),
            )


if __name__ == "__main__":
    unittest.main()
