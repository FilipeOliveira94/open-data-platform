# Agent workflow prompts

Three prompts drive one issue: plan, implement, close. They are tool-neutral; the rules live in [`AGENTS.md`](../AGENTS.md). Replace the placeholders and run the plan prompt first. A large task waits for Filipe's approval of the plan before any file changes; a small task goes straight to implementation (see "Task size" in `AGENTS.md` §3).

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

Mode: <small|large>. Keep the scope limited to this issue. Do not implement future phases or silently change architecture. Large: wait for my approval before editing. Small: implement after planning. If planning shows the task is larger than stated, reclassify it as large and stop.
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
- Apply the AGENTS.md §12 "Issue Close Checklist".
- Commit with Conventional Commits on the issue branch, push the branch, and open a PR to main. Never push or merge main (AGENTS.md §13 Git).

Return a concise completion report, the PR link, the last commit SHA, and the next smallest useful task.
```

---

## Fallback model

The default implementer is Claude Opus 5.5 in T3 Code. When the Claude Pro limit runs out, use Grok 4.7 through the Cursor provider in T3. Paste the same three prompts. Point it at `AGENTS.md`; do not keep that tool's own rule files in the repository, so the rules have one source. The git policy is in AGENTS.md §13, so the other tool gets it too.
