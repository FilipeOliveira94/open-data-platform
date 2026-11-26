# Agent workflow prompts

Three prompts drive one issue: plan, implement, close. The rules live in [`AGENTS.md`](../AGENTS.md). Replace the placeholders and run the plan prompt first. An issue rated above Sm + Lo waits for Filipe's approval of the plan before any file changes; an Sm + Lo issue goes straight to implementation (see "Size and risk" in `AGENTS.md` §3).

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
9. For a pipeline step, one answer per row of the ROADMAP.md "Pipeline design checklist", each with the test or live check that proves it. "Not applicable" needs a reason.

Rating: <Sm|Md|Lg> / <Lo|Med|Hi> (the expected half of the roadmap's Risk and Size columns; estimate it in the plan if missing). Keep the scope limited to this issue. Do not implement future phases or silently change architecture. Above Sm + Lo: run po-reviewer on the plan, show its verdict with the plan, and wait for my approval before editing. Sm + Lo: implement after planning. If planning shows more work or risk than the rating, raise it, say why, and follow the new rating.
```

## 2. Implement (straight away for Sm + Lo; after the plan is approved otherwise)

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
- Work on the issue branch from AGENTS.md §13. Do not commit; the close step commits and opens the PR.

At the end, report changed files, checks run, results, acceptance criteria met and not met, limitations, and the next recommended task.
```

## 3. Close

```text
Follow AGENTS.md.

Review the current diff against the agreed plan and acceptance criteria. Do not add features.
- Check for unrelated changes, secrets, generated files, weak tests, configuration drift and undocumented behavior.
- Run the relevant validation commands and report their actual output.
- Identify acceptance criteria that are not met or not verified.
- Run the review gates in AGENTS.md §3 "Agent workflow": code-reviewer for any code or config change; for an issue rated above Sm + Lo, then po-reviewer in acceptance mode. Fix blocking findings and put each final verdict in the PR.
- Apply the AGENTS.md §12 "Issue Close Checklist".
- Commit with Conventional Commits on the issue branch, push the branch, and open a PR to main. Never push or merge main (AGENTS.md §13 Git).

Return a concise completion report, the PR link, the last commit SHA, and the next smallest useful task.
```
