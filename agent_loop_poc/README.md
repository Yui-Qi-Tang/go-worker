# Agent Loop POC

This folder contains a minimal control plane for the workflow:

`tasker -> researcher -> planner -> implementer -> reviewer`

The POC does not run the agents. It inspects artifacts and writes a small
machine-readable state file so a runner can decide:

- which stage should run next
- whether the task is done
- whether the workflow is blocked
- whether the workflow should return to an upstream stage
- whether human input is likely required before the next loop iteration

## JSON-First Artifacts

Workflow handoff artifacts keep their historical `.md` filenames, but their
canonical content is now one valid JSON object. The loop parser reads JSON
fields first and keeps a legacy Markdown-label fallback for older artifacts.

Important JSON fields used by the POC:

- `review.md`
  - `overall_judgment`
  - `recommended_return_target`
- `result.md`
  - `status`
  - `suggested_return_target`

Legacy fallback fields:

- `Overall Judgment: ...`
- `Recommended Return Target: ...`
- `Status: ...`
- `Suggested Return Target: ...`

## State Layout

For each task, the runner stores state at:

- `share/{task_id}/loop/state.json`

Example:

```json
{
  "schema_version": "agent_loop_poc.v1",
  "task_id": "24",
  "status": "needs_revision",
  "current_stage": "reviewer",
  "next_stage": "tasker",
  "last_completed_stage": "reviewer",
  "return_target": "tasker",
  "needs_user_input": true,
  "reason": "Reviewer marked the workflow failed and routed it upstream.",
  "artifacts": {
    "tasker_input": true,
    "researcher_task": true,
    "planner_plan": true,
    "implementer_impl": true,
    "reviewer_result": true,
    "reviewer_review": true
  },
  "history": [
    {
      "event": "sync",
      "status": "needs_revision",
      "current_stage": "reviewer",
      "next_stage": "tasker"
    }
  ]
}
```

## Workflow States

- `initialized`: task input exists, but no downstream artifact exists yet
- `ready`: the next stage is known and can run now
- `running`: reserved for future integration with an actual agent executor
- `needs_revision`: a downstream artifact routed the workflow back upstream
- `waiting_user`: user clarification or approval is likely needed before
  looping
- `blocked`: the workflow cannot continue because a required input or decision
  is missing
- `completed`: reviewer marked the workflow successful

## Transition Rules

The runner uses the existing artifacts as the source of truth.

1. If `share/{task_id}/reviewer/review.md` exists:
   - JSON `overall_judgment: success` or legacy `Overall Judgment: success`
     means `completed`
   - JSON `overall_judgment: failed` or legacy `Overall Judgment: failed`
     means `needs_revision` and follows the return target
2. Else if `share/{task_id}/reviewer/result.md` exists:
   - `status: success` means `ready`, next stage `reviewer`
   - `status: failed`, `partial_failure`, or `blocked` follows
     `suggested_return_target` if present, otherwise `blocked`
3. Else if `share/{task_id}/implementer/impl.md` exists:
   - next stage is `implementer`, unless the JSON status is `blocked`
4. Else if `share/{task_id}/planner/plan.md` exists:
   - next stage is `planner`, unless the JSON status is `blocked`
5. Else if `share/{task_id}/researcher/task.md` exists:
   - next stage is `researcher`, unless the JSON status is `blocked`
6. Else if `tasker/{task_id}.md` exists:
   - next stage is `tasker`
7. Otherwise:
   - `blocked`

## Human-In-The-Loop Heuristic

The POC uses a simple heuristic:

- if a failed or blocked artifact mentions phrases like `clarify`, `approval`,
  `confirm`, `user`, or `task update`, then `needs_user_input=true`
- otherwise the runner assumes the workflow can loop to the recommended stage

This is intentionally conservative and easy to replace later.

## CLI

Supported commands:

```bash
python3 agent_loop_poc/loop.py init <task_id>
python3 agent_loop_poc/loop.py sync <task_id>
python3 agent_loop_poc/loop.py next <task_id>
python3 agent_loop_poc/loop.py status <task_id>
```

## Scope Limits

This is only a POC. It does not yet:

- invoke real stage agents automatically
- enforce fresh model invocation isolation
- enforce file locking or leases
- manage retry budgets
- support parallel tasks safely
- validate the full JSON schema beyond fields needed for routing
