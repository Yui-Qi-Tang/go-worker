from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from agent_loop_poc import loop


class LoopRoutingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.old_root = loop.ROOT
        loop.ROOT = Path(self.tmp.name)

    def tearDown(self) -> None:
        loop.ROOT = self.old_root
        self.tmp.cleanup()

    def write(self, relative: str, text: str) -> None:
        path = loop.ROOT / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")

    def write_json(self, relative: str, value: dict[str, object]) -> None:
        self.write(relative, json.dumps(value, indent=2) + "\n")

    def test_json_review_success_completes_workflow(self) -> None:
        self.write("tasker/1.md", "raw task\n")
        self.write_json(
            "share/1/reviewer/review.md",
            {
                "artifact_type": "workflow_review",
                "overall_judgment": "success",
                "recommended_return_target": "NONE",
            },
        )

        state = loop.sync_command("1")

        self.assertEqual(state["status"], "completed")
        self.assertIsNone(state["next_stage"])
        self.assertEqual(state["current_stage"], "reviewer")

    def test_json_review_failed_routes_to_return_target(self) -> None:
        self.write("tasker/2.md", "raw task\n")
        self.write_json(
            "share/2/reviewer/review.md",
            {
                "artifact_type": "workflow_review",
                "overall_judgment": "failed",
                "recommended_return_target": "tasker",
                "reason": "schema mismatch",
            },
        )

        state = loop.sync_command("2")

        self.assertEqual(state["status"], "needs_revision")
        self.assertEqual(state["next_stage"], "tasker")
        self.assertEqual(state["return_target"], "tasker")

    def test_json_result_failure_routes_to_suggested_target(self) -> None:
        self.write("tasker/3.md", "raw task\n")
        self.write_json(
            "share/3/reviewer/result.md",
            {
                "artifact_type": "implementation_result",
                "status": "failed",
                "suggested_return_target": "planner",
                "failure_type": "plan_mismatch",
            },
        )

        state = loop.sync_command("3")

        self.assertEqual(state["status"], "needs_revision")
        self.assertEqual(state["next_stage"], "planner")
        self.assertEqual(state["return_target"], "planner")

    def test_legacy_markdown_review_labels_still_parse(self) -> None:
        self.write("tasker/4.md", "raw task\n")
        self.write(
            "share/4/reviewer/review.md",
            "Overall Judgment: failed\nRecommended Return Target: planner\n",
        )

        state = loop.sync_command("4")

        self.assertEqual(state["status"], "needs_revision")
        self.assertEqual(state["next_stage"], "planner")
        self.assertEqual(state["return_target"], "planner")

    def test_valid_json_does_not_fall_back_to_embedded_markdown(self) -> None:
        self.write("tasker/5.md", "raw task\n")
        self.write_json(
            "share/5/reviewer/review.md",
            {
                "artifact_type": "workflow_review",
                "notes": ["Overall Judgment: success"],
            },
        )

        state = loop.sync_command("5")

        self.assertEqual(state["status"], "blocked")
        self.assertIsNone(state["next_stage"])

    def test_blocked_json_intermediate_artifact_stops_next_stage(self) -> None:
        self.write("tasker/6.md", "raw task\n")
        self.write_json(
            "share/6/researcher/task.md",
            {
                "artifact_type": "normalized_task",
                "status": "blocked",
                "open_questions": [{"question": "Needs approval"}],
            },
        )

        state = loop.sync_command("6")

        self.assertEqual(state["status"], "waiting_user")
        self.assertIsNone(state["next_stage"])
        self.assertTrue(state["needs_user_input"])


if __name__ == "__main__":
    unittest.main()
