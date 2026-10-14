# Agent kickoff prompt

Use this prompt at the start of an implementation task. Replace the issue placeholder, then wait for the plan before approving any code changes.

---

Follow the project instructions in `AGENTS.md` and the Cursor rules in `.cursor/rules/`.

I want to work on issue: `<ISSUE-ID> — <title>`.

First, inspect the relevant code, documentation, roadmap and current project status. Do not edit files yet.

Return:

1. Current state and relevant files.
2. Dependencies and prerequisites.
3. Proposed implementation steps.
4. Acceptance criteria and how each will be verified.
5. Risks, assumptions and compatibility concerns.
6. Commands you intend to run.
7. Expected files to create or modify.

Keep the scope limited to this issue. Do not implement future phases.

Wait for my approval before starting implementation.
