import json
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from pipeline_reporting import (
    DailyReportStore,
    replace_report_rows,
    session_date_from_target,
)


class PipelineReportingTests(unittest.TestCase):
    def test_session_date_uses_exchange_timezone(self):
        self.assertEqual(
            session_date_from_target(
                "2025-07-03T17:00:00+00:00",
                "XNYS",
            ),
            "2025-07-03",
        )

    def test_report_is_saved_to_latest_and_session_history(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            store = DailyReportStore(
                Path(temporary_directory),
                "2025-07-03",
                "parquet",
            )
            dataframe = pd.DataFrame([{"symbol": "AAPL", "status": "ok"}])

            latest, history = store.save_dataframe(
                dataframe,
                "quality_summary.csv",
            )
            store.save_json({"status": "success"}, "run_summary.json")

            self.assertTrue(latest.is_file())
            self.assertTrue(history.is_file())
            self.assertEqual(
                json.loads(
                    (store.latest_root / "run_summary.json").read_text(
                        encoding="utf-8"
                    )
                )["status"],
                "success",
            )

    def test_deep_reports_are_separated_and_pruned_earlier(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            old_summary = root / "history" / "2025-06-01" / "parquet"
            old_summary.mkdir(parents=True)
            (old_summary / "quality_summary.csv").write_text("ok\n", encoding="utf-8")
            old_details = old_summary / "deep_quality"
            old_details.mkdir()
            (old_details / "missing.csv").write_text("gap\n", encoding="utf-8")

            expired = root / "history" / "2024-06-01" / "parquet"
            expired.mkdir(parents=True)
            (expired / "quality_summary.csv").write_text("old\n", encoding="utf-8")

            store = DailyReportStore(root, "2025-07-03", "parquet")
            store.prune_history(
                summary_retention_days=365,
                detailed_retention_days=30,
            )

            self.assertTrue((old_summary / "quality_summary.csv").is_file())
            self.assertFalse(old_details.exists())
            self.assertFalse(expired.parent.exists())

    def test_rerun_keeps_skipped_rows_and_replaces_checked_symbol(self):
        previous = pd.DataFrame(
            [
                {"symbol": "AAPL", "status": "old"},
                {"symbol": "MSFT", "status": "ok"},
            ]
        )
        current = pd.DataFrame([{"symbol": "AAPL", "status": "ok"}])

        merged = replace_report_rows(
            previous,
            current,
            "symbol",
            {"AAPL"},
        ).set_index("symbol")

        self.assertEqual(merged.loc["AAPL", "status"], "ok")
        self.assertEqual(merged.loc["MSFT", "status"], "ok")


if __name__ == "__main__":
    unittest.main()
