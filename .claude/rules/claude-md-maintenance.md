---
description: Rules for keeping CLAUDE.md and README.md files up to date after code changes in InOutModule/
---

After making any code changes in `InOutModule/`, update the relevant documentation files.

**`CLAUDE.md`** — context for Claude that is hard to derive from reading the code: non-obvious patterns, gotchas, architectural decisions, naming conventions, and implementation constraints.

**`README.md`** — human-facing documentation: usage instructions, API overview, key concepts, and examples.

**Rules:**
- Update the file(s) closest to the code that changed.
- If a change also affects `LEGO/` or root-level code, update the corresponding docs there too (see root `.claude/rules/claude-md-maintenance.md` for the full index).
- Do not duplicate content between `README.md` and `CLAUDE.md` — if something is in `README.md`, `CLAUDE.md` should reference it, not repeat it.
- Use prose references (`See README.md for ...`) to point to large human-facing docs, not `@`-imports.
- Delete stale entries rather than commenting them out.

**Index of documentation files in InOutModule/:**

| File                    | Audience | Contents                                                                |
|-------------------------|----------|-------------------------------------------------------------------------|
| `InOutModule/CLAUDE.md` | Claude   | CaseStudy read order, ExcelReader/Writer patterns, SQLiteWriter, Caller |
| `InOutModule/README.md` | Humans   | CaseStudy API, SQLiteWriter usage, Caller, Excel file format            |
