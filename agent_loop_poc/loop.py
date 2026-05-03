#!/usr/bin/env python3
"""Minimal orchestrator for the JSON-first workflow."""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent.parent
SCHEMA_VERSION = "agent_loop_poc.v1"
STAGES = ("tasker", "researcher", "planner", "implementer", "reviewer")
HUMAN_HINTS = (
    "clarify",
    "approval",
    "confirm",
    "user",
    "task update",
    "open_questions",
    "requires_user_approval",
)


@dataclass
class TaskPaths:
    task_id: str
    tasker_input: Path
    researcher_task: Path
    planner_plan: Path
    implementer_impl: Path
    reviewer_result: Path
    reviewer_review: Path
    state_file: Path


def build_paths(task_id: str) -> TaskPaths:
    share_root = ROOT / "share" / task_id
    return TaskPaths(
        task_id=task_id,
        tasker_input=ROOT / "tasker" / f"{task_id}.md",
        researcher_task=share_root / "researcher" / "task.md",
        planner_plan=share_root / "planner" / "plan.md",
        implementer_impl=share_root / "implementer" / "impl.md",
        reviewer_result=share_root / "reviewer" / "result.md",
        reviewer_review=share_root / "reviewer" / "review.md",
        state_file=share_root / "loop" / "state.json",
    )


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def extract_field(text: str, field: str) -> str | None:
    pattern = re.compile(rf"^{re.escape(field)}:\s*(.+?)\s*$", re.MULTILINE)
    match = pattern.search(text)
    if not match:
        return None
    return match.group(1).strip()


def parse_json_artifact(text: str) -> dict[str, Any] | None:
    try:
        value = json.loads(text)
    except json.JSONDecodeError:
        return None
    if not isinstance(value, dict):
        return None
    return value


def get_nested(data: dict[str, Any] | None, key: str) -> Any:
    if data is None:
        return None
    value: Any = data
    for part in key.split("."):
        if not isinstance(value, dict) or part not in value:
            return None
        value = value[part]
    return value


def stringify_field(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        return value or None
    return str(value).strip() or None


def artifact_field(
    text: str,
    data: dict[str, Any] | None,
    *,
    json_keys: tuple[str, ...],
    markdown_field: str,
) -> str | None:
    for key in json_keys:
        value = stringify_field(get_nested(data, key))
        if value is not None:
            return value
    if data is None:
        return extract_field(text, markdown_field)
    return None


def normalize_status(value: str | None) -> str | None:
    if value is None:
        return None
    return value.strip().lower()


def normalize_target(value: str | None) -> str | None:
    if value is None:
        return None
    target = value.strip().lower()
    if target in {"", "none", "null", "unknown", "not_applicable"}:
        return None
    return target


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def bool_artifacts(paths: TaskPaths) -> dict[str, bool]:
    return {
        "tasker_input": paths.tasker_input.exists(),
        "researcher_task": paths.researcher_task.exists(),
        "planner_plan": paths.planner_plan.exists(),
        "implementer_impl": paths.implementer_impl.exists(),
        "reviewer_result": paths.reviewer_result.exists(),
        "reviewer_review": paths.reviewer_review.exists(),
    }


def history_event(
    state: dict[str, Any],
    *,
    event: str,
    status: str,
    current_stage: str | None,
    next_stage: str | None,
    reason: str,
) -> None:
    state.setdefault("history", []).append(
        {
            "at": now_iso(),
            "event": event,
            "status": status,
            "current_stage": current_stage,
            "next_stage": next_stage,
            "reason": reason,
        }
    )


def load_state(paths: TaskPaths) -> dict[str, Any] | None:
    if not paths.state_file.exists():
        return None
    return json.loads(paths.state_file.read_text(encoding="utf-8"))


def save_state(paths: TaskPaths, state: dict[str, Any]) -> None:
    paths.state_file.parent.mkdir(parents=True, exist_ok=True)
    paths.state_file.write_text(json.dumps(state, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")


def infer_waiting_user(*texts: str) -> bool:
    haystack = " ".join(texts).lower()
    return any(hint in haystack for hint in HUMAN_HINTS)


def stage_after(stage: str) -> str | None:
    try:
        idx = STAGES.index(stage)
    except ValueError:
        return None
    if idx == len(STAGES) - 1:
        return None
    return STAGES[idx + 1]


def sync_state(paths: TaskPaths, existing: dict[str, Any] | None = None) -> dict[str, Any]:
    artifacts = bool_artifacts(paths)
    state = existing or {
        "schema_version": SCHEMA_VERSION,
        "task_id": paths.task_id,
        "status": "initialized",
        "current_stage": None,
        "next_stage": None,
        "last_completed_stage": None,
        "return_target": None,
        "needs_user_input": False,
        "reason": "State initialized.",
        "artifacts": artifacts,
        "history": [],
        "updated_at": now_iso(),
    }
    state["schema_version"] = SCHEMA_VERSION
    state["task_id"] = paths.task_id
    state["artifacts"] = artifacts

    status = "blocked"
    current_stage = None
    next_stage = None
    last_completed_stage = None
    return_target = None
    needs_user_input = False
    reason = "No recognized workflow entrypoint was found."

    if paths.reviewer_review.exists():
        text = read_text(paths.reviewer_review)
        artifact = parse_json_artifact(text)
        judgment = normalize_status(
            artifact_field(
                text,
                artifact,
                json_keys=("overall_judgment", "status"),
                markdown_field="Overall Judgment",
            )
        )
        target = normalize_target(
            artifact_field(
                text,
                artifact,
                json_keys=("recommended_return_target", "handoff.next_agent"),
                markdown_field="Recommended Return Target",
            )
        )
        current_stage = "reviewer"
        last_completed_stage = "reviewer"
        if judgment == "success":
            status = "completed"
            reason = "Reviewer marked the workflow successful."
        elif judgment == "failed":
            return_target = target
            next_stage = return_target
            status = "needs_revision" if return_target else "blocked"
            needs_user_input = infer_waiting_user(text)
            reason = "Reviewer marked the workflow failed and routed it upstream." if return_target else "Reviewer failed the workflow without a return target."
    elif paths.reviewer_result.exists():
        text = read_text(paths.reviewer_result)
        artifact = parse_json_artifact(text)
        result_status = normalize_status(
            artifact_field(
                text,
                artifact,
                json_keys=("status",),
                markdown_field="Status",
            )
        )
        target = normalize_target(
            artifact_field(
                text,
                artifact,
                json_keys=("suggested_return_target", "handoff.next_agent"),
                markdown_field="Suggested Return Target",
            )
        )
        current_stage = "implementer"
        last_completed_stage = "implementer"
        if result_status == "success":
            status = "ready"
            next_stage = "reviewer"
            reason = "Implementer completed successfully; reviewer should run next."
        elif result_status in {"failed", "partial_failure", "blocked"}:
            return_target = target
            next_stage = return_target
            status = "needs_revision" if return_target else "blocked"
            needs_user_input = infer_waiting_user(text)
            reason = "Implementer reported a failure and suggested returning upstream." if return_target else "Implementer reported a failure without a return target."
    elif paths.implementer_impl.exists():
        text = read_text(paths.implementer_impl)
        artifact = parse_json_artifact(text)
        artifact_status = normalize_status(
            artifact_field(
                text,
                artifact,
                json_keys=("status",),
                markdown_field="Status",
            )
        )
        current_stage = "planner"
        last_completed_stage = "planner"
        if artifact_status == "blocked":
            status = "blocked"
            needs_user_input = infer_waiting_user(text)
            reason = "Implementation brief is blocked; implementer should not run."
        else:
            next_stage = "implementer"
            status = "ready"
            reason = "Implementation brief exists; implementer should run next."
    elif paths.planner_plan.exists():
        text = read_text(paths.planner_plan)
        artifact = parse_json_artifact(text)
        artifact_status = normalize_status(
            artifact_field(
                text,
                artifact,
                json_keys=("status",),
                markdown_field="Status",
            )
        )
        current_stage = "researcher"
        last_completed_stage = "researcher"
        if artifact_status == "blocked":
            status = "blocked"
            needs_user_input = infer_waiting_user(text)
            reason = "Research plan is blocked; planner should not run."
        else:
            next_stage = "planner"
            status = "ready"
            reason = "Research plan exists; planner should run next."
    elif paths.researcher_task.exists():
        text = read_text(paths.researcher_task)
        artifact = parse_json_artifact(text)
        artifact_status = normalize_status(
            artifact_field(
                text,
                artifact,
                json_keys=("status",),
                markdown_field="Status",
            )
        )
        current_stage = "tasker"
        last_completed_stage = "tasker"
        if artifact_status == "blocked":
            status = "blocked"
            needs_user_input = infer_waiting_user(text)
            reason = "Normalized task is blocked; researcher should not run."
        else:
            next_stage = "researcher"
            status = "ready"
            reason = "Normalized task exists; researcher should run next."
    elif paths.tasker_input.exists():
        next_stage = "tasker"
        status = "initialized"
        reason = "Task input exists; tasker should run first."
    else:
        needs_user_input = True
        reason = f"Missing task input file: {paths.tasker_input}"

    if status in {"needs_revision", "blocked"} and needs_user_input:
        status = "waiting_user"

    state["status"] = status
    state["current_stage"] = current_stage
    state["next_stage"] = next_stage
    state["last_completed_stage"] = last_completed_stage
    state["return_target"] = return_target
    state["needs_user_input"] = needs_user_input
    state["reason"] = reason
    state["updated_at"] = now_iso()
    history_event(
        state,
        event="sync",
        status=status,
        current_stage=current_stage,
        next_stage=next_stage,
        reason=reason,
    )
    return state


def init_state(task_id: str) -> dict[str, Any]:
    paths = build_paths(task_id)
    state = sync_state(paths)
    history_event(
        state,
        event="init",
        status=state["status"],
        current_stage=state["current_stage"],
        next_stage=state["next_stage"],
        reason=state["reason"],
    )
    save_state(paths, state)
    return state


def sync_command(task_id: str) -> dict[str, Any]:
    paths = build_paths(task_id)
    existing = load_state(paths)
    state = sync_state(paths, existing=existing)
    save_state(paths, state)
    return state


def print_compact(state: dict[str, Any]) -> None:
    print(json.dumps(state, indent=2, ensure_ascii=True))


def main() -> int:
    parser = argparse.ArgumentParser(description="Minimal orchestrator for the JSON-first workflow.")
    sub = parser.add_subparsers(dest="command", required=True)

    for name in ("init", "sync", "next", "status"):
        cmd = sub.add_parser(name)
        cmd.add_argument("task_id")

    args = parser.parse_args()

    if args.command == "init":
        state = init_state(args.task_id)
        print_compact(state)
        return 0

    state = sync_command(args.task_id)

    if args.command == "sync":
        print_compact(state)
        return 0

    if args.command == "status":
        print(
            f"task={state['task_id']} status={state['status']} "
            f"current={state['current_stage']} next={state['next_stage']} "
            f"needs_user_input={state['needs_user_input']}"
        )
        print(state["reason"])
        return 0

    if args.command == "next":
        if state["status"] == "completed":
            print("done")
        elif state["status"] in {"waiting_user", "blocked"}:
            print("wait")
        else:
            print(state["next_stage"] or "wait")
        print(state["reason"])
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
