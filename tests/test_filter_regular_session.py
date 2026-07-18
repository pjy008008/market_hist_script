import unittest
from unittest.mock import patch

import pandas as pd

from data_filtering.filter_regular_session import (
    choose_data_type,
    choose_storage_format,
    filter_regular_session,
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


class InteractiveSelectionTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
