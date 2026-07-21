import tempfile
import unittest
from pathlib import Path

import pandas as pd

from data_filtering.resample_sip_5min import (
    ResampleSource,
    build_resample_source,
    process_resample_file_incrementally,
    process_resample_source,
    resample_sip_five_minutes,
)


class ResampleSipFiveMinutesTests(unittest.TestCase):
    @staticmethod
    def make_frame(timestamps: list[str]) -> pd.DataFrame:
        index = pd.MultiIndex.from_arrays(
            [
                ["AAP"] * len(timestamps),
                pd.to_datetime(timestamps, utc=True),
            ],
            names=["symbol", "timestamp"],
        )
        return pd.DataFrame(
            {
                "open": [10, 11, 12, 13, 14][: len(timestamps)],
                "high": [11, 13, 13, 15, 15][: len(timestamps)],
                "low": [9, 10, 11, 12, 13][: len(timestamps)],
                "close": [10.5, 12, 12.5, 14, 14.5][: len(timestamps)],
                "volume": [100, 200, 300, 400, 500][: len(timestamps)],
                "trade_count": [1, 2, 3, 4, 5][: len(timestamps)],
                "vwap": [10, 11, 12, 13, 14][: len(timestamps)],
            },
            index=index,
        )

    def test_aggregates_ohlcv_trade_count_and_weighted_vwap(self):
        dataframe = self.make_frame(
            [
                "2025-02-03 14:30:00+00:00",
                "2025-02-03 14:31:00+00:00",
                "2025-02-03 14:32:00+00:00",
                "2025-02-03 14:33:00+00:00",
                "2025-02-03 14:34:00+00:00",
            ]
        )

        result = resample_sip_five_minutes(dataframe)
        bar = result.iloc[0]

        self.assertEqual(len(result), 1)
        self.assertEqual(bar["open"], 10)
        self.assertEqual(bar["high"], 15)
        self.assertEqual(bar["low"], 9)
        self.assertEqual(bar["close"], 14.5)
        self.assertEqual(bar["volume"], 1500)
        self.assertEqual(bar["trade_count"], 15)
        self.assertAlmostEqual(bar["vwap"], 12.6666666667)
        self.assertEqual(bar["source_minutes"], 5)

    def test_missing_trade_minute_is_not_filled(self):
        dataframe = self.make_frame(
            [
                "2025-02-03 14:30:00+00:00",
                "2025-02-03 14:31:00+00:00",
                "2025-02-03 14:33:00+00:00",
                "2025-02-03 14:34:00+00:00",
            ]
        )

        result = resample_sip_five_minutes(dataframe)

        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["source_minutes"], 4)
        self.assertEqual(result.iloc[0]["volume"], 1000)

    def test_raw_source_and_destination_use_raw_directories(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            source = build_resample_source(
                "parquet",
                source_root=root / "one_minute",
                destination_root=root / "five_minute",
                data_type="raw",
            )

            self.assertEqual(
                source.source_dir,
                root / "one_minute" / "raw" / "parquet",
            )
            self.assertEqual(
                source.destination_dir,
                root / "five_minute" / "raw" / "parquet",
            )

    def test_csv_source_is_saved_with_five_minute_file_name(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            source = ResampleSource(
                storage_format="csv",
                source_dir=root / "input",
                destination_dir=root / "output",
            )
            source.source_dir.mkdir(parents=True)
            input_path = source.source_dir / "AAP_1min_sip_historical.csv"
            self.make_frame(
                [
                    "2025-02-03 14:30:00+00:00",
                    "2025-02-03 14:31:00+00:00",
                    "2025-02-03 14:32:00+00:00",
                    "2025-02-03 14:33:00+00:00",
                    "2025-02-03 14:34:00+00:00",
                ]
            ).to_csv(input_path, index=True)

            totals = process_resample_source(source)

            self.assertEqual(totals, (1, 5, 1))
            output_path = source.destination_dir / "AAP_5min_sip_historical.csv"
            self.assertTrue(output_path.is_file())
            result = pd.read_csv(output_path)
            self.assertEqual(result.loc[0, "source_minutes"], 5)

    def test_incremental_resample_rebuilds_last_session_and_appends_new_one(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            input_path = root / "input.csv"
            output_path = root / "output.csv"
            first_session = self.make_frame(
                [
                    "2025-02-03 14:30:00+00:00",
                    "2025-02-03 14:31:00+00:00",
                    "2025-02-03 14:32:00+00:00",
                    "2025-02-03 14:33:00+00:00",
                    "2025-02-03 14:34:00+00:00",
                ]
            )
            resample_sip_five_minutes(first_session).to_csv(output_path, index=True)
            second_session = first_session.copy()
            second_session.index = pd.MultiIndex.from_arrays(
                [
                    ["AAP"] * len(second_session),
                    pd.to_datetime(
                        [
                            "2025-02-04 14:30:00+00:00",
                            "2025-02-04 14:31:00+00:00",
                            "2025-02-04 14:32:00+00:00",
                            "2025-02-04 14:33:00+00:00",
                            "2025-02-04 14:34:00+00:00",
                        ],
                        utc=True,
                    ),
                ],
                names=["symbol", "timestamp"],
            )
            pd.concat([first_session, second_session]).to_csv(input_path, index=True)

            totals = process_resample_file_incrementally(
                input_path,
                output_path,
                "csv",
                pd.Timestamp("2025-02-01 00:00:00+00:00"),
                pd.Timestamp("2025-02-05 00:00:00+00:00"),
            )

            self.assertEqual(totals, (10, 2, 2))
            result = pd.read_csv(output_path)
            self.assertEqual(len(result), 2)


if __name__ == "__main__":
    unittest.main()
