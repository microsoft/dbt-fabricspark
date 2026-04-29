---
name: generate-changelog
description: Generate a succinct CHANGELOG.md entry from PR descriptions since the last version bump.
---

# Generate Changelog

Produce a concise, user-facing changelog entry for the current version by summarizing merged PR descriptions.

## Steps

1. **Check for the raw changelog** — verify that `.temp/VERSION_CHANGELOG_FULL.md` exists in the repo root:

   ```bash
   test -f .temp/VERSION_CHANGELOG_FULL.md
   ```

   If it does **not** exist, stop and tell the user:

   > The raw PR changelog has not been generated yet. Run this first:
   >
   > ```bash
   > npm run get-version-diffs
   > ```
   >
   > Then re-invoke this skill.

2. **Read the raw changelog** — open `.temp/VERSION_CHANGELOG_FULL.md`. Each entry has the format:

   ```
   # <short sha>: <commit title>

   <PR description body>

   ---
   ```

3. **Filter out sentinel commits** — skip any entry whose commit title matches the pattern `ci: sentinel commit`. These are CI verification commits with no user-facing changes.

4. **Read the current version** — get the version string from:

   ```bash
   cat src/dbt/adapters/fabricspark/__version__.py
   ```

   The file contains a single line like `version = "1.9.6"`. Extract the version number (e.g., `1.9.6`).

5. **Summarize into a changelog entry** — from the remaining (non-sentinel) PR descriptions, write a **succinct** changelog section. Follow these guidelines:

   - Group related changes under descriptive subheadings (e.g., `### Bug Fixes`, `### New Features`, `### Improvements`)
   - Each item should be 1–2 sentences max — distill the PR description down to what matters to a user
   - Use past tense (e.g., "Added…", "Fixed…", "Improved…")
   - Reference PR numbers inline (e.g., `(#42)`)
   - Do NOT include the full PR body text — summarize it
   - Match the style and depth of existing entries in `CHANGELOG.md`

6. **Prepend to CHANGELOG.md** — insert the new section immediately below the `# Changelog` heading in `CHANGELOG.md`, above all existing version sections. The format is:

   ```markdown
   ## v<version>

   <your summarized content>

   ---
   ```

   Keep the existing content intact below the new section.

7. **Report completion** — show the user the new section you added and confirm the file was updated.
