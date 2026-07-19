import json
import tempfile
import unittest
from pathlib import Path

from pipeline_state import PipelineStateStore


class PipelineStateStoreTests(unittest.TestCase):
    def test_stage_checkpoint_persists_and_requires_output_file(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            state_path = root / "state.json"
            output_path = root / "AAPL.parquet"
            store = PipelineStateStore(state_path)
            store.mark_stage(
                "parquet",
                "AAPL",
                "filter",
                "success",
                "2025-02-03T21:00:00+00:00",
                1,
            )

            self.assertFalse(
                store.is_complete(
                    "parquet",
                    "AAPL",
                    "filter",
                    "2025-02-03T21:00:00+00:00",
                    output_path,
                )
            )
            output_path.touch()
            reloaded = PipelineStateStore(state_path)
            self.assertTrue(
                reloaded.is_complete(
                    "parquet",
                    "AAPL",
                    "filter",
                    "2025-02-03T21:00:00+00:00",
                    output_path,
                )
            )

            payload = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(
                payload["checkpoints"]["parquet"]["AAPL"]["filter"]["status"],
                "success",
            )

    def test_run_summary_records_failures(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            state_path = Path(temporary_directory) / "state.json"
            store = PipelineStateStore(state_path)
            store.begin_run("csv", "2025-02-03T21:00:00+00:00")
            failures = [{"stage": "collection", "symbol": "AAPL"}]
            store.finish_run("failed", failures)

            reloaded = PipelineStateStore(state_path)
            self.assertEqual(reloaded.data["last_run"]["status"], "failed")
            self.assertEqual(reloaded.data["last_run"]["failure_count"], 1)


if __name__ == "__main__":
    unittest.main()
