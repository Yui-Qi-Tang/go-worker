# go-worker

    A simple worker manager with consistency task interface and fail recovery.

# Development

    go version: 1.14+

# Example

Single worker

```go
    // create worker
    worker, err := worker.NewWorker(WithName("my-worker"))
	if err != nil {
		// check error here
    }
    
    worker.Start() // start worker and wait task

    err := worker.Do(task) // task need to implement worker.Task interface

    if err != nil {
        // check error here...
    }

    // ...

    worker.Stop() // stop if task ok
    
```

Multiple workers are managed by Master

```go

    // ms, err := NewMaster(WithWorkerRecovery(true)) // master support worker recovery
    ms, err := NewMaster() // default master
	if err != nil {
		panic(err) 
	}

	if err := ms.AddWorkers(workers); err != nil {
        panic(err) 
    }

	if err := ms.WakeAllWorkersUp(); err != nil {
		if err == ErrMasterWorkerPoolIsEmpty {
			// handle the specify error
		}
		panic(err) // unexcepted error, so panic
	}


	for i := 0; i < 1000; i++ {
		ms.Schedule(task[i]) // where for all task in []task that implements task interface
    }
    
    // ...

	ms.Stop() // call stop if all task ok

```


# Workflow README

A workflow help to develop

`tasker -> researcher -> planner -> implementer -> reviewer`

The README files are explanatory only. Workflow rules, output schemas, and
stage boundaries are defined by the root `AGENTS.md` and the nearest
stage-specific `AGENTS.md`.

## Folder Map

- `tasker/{task_id}.md`: user-facing task input
- `share/{task_id}/researcher/task.md`: `normalized_task` JSON artifact
- `share/{task_id}/planner/plan.md`: `research_plan` JSON artifact
- `share/{task_id}/implementer/impl.md`: `implementation_brief` JSON artifact
- `share/{task_id}/reviewer/result.md`: `implementation_result` JSON artifact
- `share/{task_id}/reviewer/review.md`: `workflow_review` JSON artifact
- `share/{task_id}/reviewer/{task.md,plan.md,impl.md,result.md,review.md}`:
  self-contained review bundle
- `share/{task_id}/loop/`: optional loop-state and follow-up artifacts

The artifact filenames still end in `.md` for migration safety, but the handoff
content is one valid JSON object, not free-form Markdown.

## Artifact Boundary

JSON artifacts are the handoff boundary between agents. Every handoff artifact
must include the shared fields defined in root `AGENTS.md`, including:

- `schema_version`
- `artifact_type`
- `task_id`
- `produced_by`
- `status`
- `input_artifacts`
- `trusted_sources`
- `untrusted_inputs_seen`
- `constraints`
- `open_questions`
- `evidence`
- `handoff`

Each stage-specific `AGENTS.md` defines the additional fields and allowed enum
values for that stage.

## Agent Responsibilities

| Agent | Artifact | Responsibility | Boundary |
| --- | --- | --- | --- |
| `tasker` | `normalized_task` | Normalize the raw request into a bounded task spec. | Must block on conflicts and must not invent missing requirements. |
| `researcher` | `research_plan` | Restate the task, classify it, surface risks, and identify likely failure modes. | Must not write code or relax constraints. |
| `planner` | `implementation_brief` | Turn the task into ordered implementation steps, invariants, checks, and escalation rules. | Must not hide upstream gaps or produce vague guidance. |
| `implementer` | `implementation_result` | Execute conservatively, validate, clean up, and record what changed. | Must not expand scope or silently rewrite the plan. |
| `reviewer` | `workflow_review` | Judge success, verify artifacts, identify the earliest likely failure source, and route the loop. | Must not fix code or invent evidence. |

## Context Isolation

The workflow is designed for fresh model invocations at each stage.

Real stage-to-stage forgetting only happens if the runner starts a new model
invocation and passes only the allowed files:

- root `AGENTS.md`
- the stage-specific `AGENTS.md`
- the current stage input artifact
- any files explicitly authorized by that artifact

A prompt that says "forget the previous conversation" inside the same ongoing
chat is only a soft instruction. It does not guarantee that prior conversation
state or hidden reasoning has been removed. The safer design is to keep the
handoff artifact small, structured, and explicit.

JSON artifacts still help even when perfect isolation is unavailable because
they make drift, missing evidence, invalid enum values, and unsupported claims
easier to detect.

## Writing Task Inputs

The user-facing task file can be natural language Markdown. It should still be
explicit about:

- the final goal
- in-scope and out-of-scope files or systems
- source-of-truth files, pages, tickets, or examples
- constraints that must not be relaxed
- deliverables
- validation expectations
- cleanup or rollback expectations
- open questions that must be resolved before implementation

If a decision affects implementation, validation, scope, cleanup, rollback, or
reporting, put it in `tasker/{task_id}.md` instead of leaving it for downstream
agents to guess.

## Loop POC

The repo contains a lightweight loop POC:

- `agent_loop_poc/README.md`
- `agent_loop_poc/loop.py`

Example commands:

```bash
python3 agent_loop_poc/loop.py init 24
python3 agent_loop_poc/loop.py sync 24
python3 agent_loop_poc/loop.py next 24
python3 agent_loop_poc/loop.py status 24
```

The POC reads JSON artifacts first and keeps a legacy Markdown-label fallback
for older artifacts.

## Healthy Workflow Rules

- Treat `AGENTS.md` files as authoritative and README files as explanatory.
- Keep the review bundle self-contained before the reviewer starts.
- Use JSON artifacts as the only handoff between stages.
- Do not rely on chat memory between stages.
- Do not invent validation, cleanup, rollback, or approval rules.
- Do not silently relax constraints to make implementation easier.
- If a task needs a rule change, update the task first and rerun the pipeline.

## Where To Look Next

- Root workflow rules: `./AGENTS.md`
- Stage rules: `./tasker/AGENTS.md`, `./researcher/AGENTS.md`,
  `./planner/AGENTS.md`, `./implementer/AGENTS.md`,
  `./reviewer/AGENTS.md`
- Loop POC: `./agent_loop_poc/README.md`