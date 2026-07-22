import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import call, patch

from pipeline_state import PipelineStateStore, replace_with_retry


class PipelineStateStoreTests(unittest.TestCase):
    def test_multi_output_checkpoint_requires_every_file(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            state = PipelineStateStore(root / "state.json")
            outputs = [root / "one.parquet", root / "four.parquet"]
            for output in outputs:
                output.write_text("ok", encoding="utf-8")
            state.mark_stage(
                "parquet",
                "AAPL",
                "long_term_collection",
                "success",
                "2026-01-02T21:00:00+00:00",
                1,
            )
            self.assertTrue(
                state.is_complete_outputs(
                    "parquet",
                    "AAPL",
                    "long_term_collection",
                    "2026-01-02T21:00:00+00:00",
                    outputs,
                )
            )
            outputs[1].unlink()
            self.assertFalse(
                state.is_complete_outputs(
                    "parquet",
                    "AAPL",
                    "long_term_collection",
                    "2026-01-02T21:00:00+00:00",
                    outputs,
                )
            )

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

    @patch("pipeline_state.time.sleep")
    @patch("pipeline_state.os.replace")
    def test_replace_retries_transient_permission_error(
        self,
        mock_replace,
        mock_sleep,
    ):
        mock_replace.side_effect = [
            PermissionError(5, "access denied"),
            PermissionError(5, "access denied"),
            None,
        ]

        replace_with_retry(Path("state.tmp"), Path("state.json"))

        self.assertEqual(mock_replace.call_count, 3)
        self.assertEqual(
            mock_sleep.call_args_list,
            [call(0.1), call(0.2)],
        )

    @patch("pipeline_state.time.sleep")
    @patch(
        "pipeline_state.os.replace",
        side_effect=PermissionError(5, "access denied"),
    )
    def test_save_cleans_process_temp_file_after_retry_exhaustion(
        self,
        mock_replace,
        mock_sleep,
    ):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            store = PipelineStateStore(root / "state.json")

            with self.assertRaises(PermissionError):
                store.save()

            self.assertEqual(mock_replace.call_count, 6)
            self.assertEqual(mock_sleep.call_count, 5)
            self.assertEqual(list(root.glob(".state.json.*.tmp")), [])


if __name__ == "__main__":
    unittest.main()
