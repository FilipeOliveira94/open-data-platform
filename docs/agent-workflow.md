# Agent workflow prompts

Three prompts drive one issue: plan, implement, close. They are tool-neutral; the rules live in [`AGENTS.md`](../AGENTS.md). Replace the placeholders, run the plan prompt first, and approve the plan before any file changes.

---

## 1. Plan (no file edits)

```text
Follow AGENTS.md.

I want to work on issue: <ISSUE-ID> — <title>.

First inspect the relevant code, documentation, roadmap, acceptance criteria and current project status. Do not edit files yet.

Return:
1. Current state and relevant files.
2. Dependencies and prerequisites.
3. A small, ordered implementation plan.
4. Acceptance criteria and how each will be verified.
5. Risks, assumptions and compatibility concerns.
6. Exact commands to run.
7. Expected files to create or modify.
8. Which items of the AGENTS.md §12 "Issue Close Checklist" this issue will touch, and which §8 "Runtime data exposure" rules it touches.

Keep the scope limited to this issue. Do not implement future phases or silently change architecture. Wait for my approval before editing.
```

## 2. Implement (after the plan is approved)

```text
Follow AGENTS.md.

Implement only the plan we just agreed on.
- Keep the diff focused. Explain any necessary deviation before making it.
- Add or update tests for the behavior changed.
- Do not implement tasks from future phases.
- Do not add dependencies or services outside the approved plan.
- Do not run destructive commands, deploy cloud resources, or incur cloud costs without my explicit approval.
- Run the relevant checks and report the actual output.
- Update documentation and PROJECT_STATUS.md only for work that has actually been verified.
- Do not commit; the close step commits.

At the end, report changed files, checks run, results, acceptance criteria met and not met, limitations, and the next recommended task.
```

## 3. Close

```text
Follow AGENTS.md.

Review the current diff against the agreed plan and acceptance criteria. Do not add features.
- Check for unrelated changes, secrets, generated files, weak tests, configuration drift and undocumented behavior.
- Run the relevant validation commands and report their actual output.
- Identify acceptance criteria that are not met or not verified.
- Apply the AGENTS.md §12 "Issue Close Checklist".
- Make one local Conventional Commit on main. Never push (AGENTS.md §13 Git).

Return a concise completion report, the commit SHA, and the next smallest useful task.
```

---

## Fallback to another tool (e.g. Cursor)

Paste the same three prompts into the other tool. Point it at `AGENTS.md`; do not keep that tool's own rule files in the repository, so the rules have one source. The git policy is in AGENTS.md §13, so the other tool gets it too.
