---
name: compact-dependabot
description: Compact open Dependabot PRs into a single branch, run CI, and open a consolidated PR.
---

# Compact Dependabot

Merge all open Dependabot dependency bumps into one PR that passes CI.

## Steps

1. **Fetch the diffs** — run the TypeScript tool to download Dependabot PR diffs:

   ```bash
   npx tsx tools/gh-automation/compact-dependabot.ts
   ```

   This writes `dependabot-diffs.patch` in the repo root.

2. **Study the patch file** — read `dependabot-diffs.patch` to understand what dependencies are being bumped and across which ecosystems (npm, uv/pip, etc.).

3. **Create a branch** from the latest `origin/main`:

   ```bash
   git fetch origin main
   git checkout -b chore/compact-dependabot origin/main
   ```

4. **Apply the diffs** — apply each PR's diff from the patch file. The patch file contains sections separated by comment headers (`# PR #<number>: <title>`). Apply them with:

   ```bash
   git apply dependabot-diffs.patch
   ```

   If `git apply` fails for any section, split the patch by PR section and apply individually. Resolve any conflicts manually.

5. **Install updated dependencies** — run the appropriate install commands for each ecosystem that changed:

   - **npm**: `npm install`
   - **uv/pip**: `uv lock` or `uv sync`

6. **Run build**:

   ```bash
   npx nx affected --base=origin/main -t build --parallel=3 --configuration=ci --output-style=stream --verbose
   ```

7. **Run lint**:

   ```bash
   npx nx affected --base=origin/main -t lint --parallel=3 --configuration=ci --verbose --output-style=stream --verbose
   ```

8. **Fix failures** — if build or lint fail, read the errors and fix them. Re-run until green.

9. **Commit and push**:

   ```bash
   git add -A
   git commit -m "chore: compact dependabot bumps"
   git push origin chore/compact-dependabot
   ```

10. **Open a PR**:

    ```bash
    gh pr create --title "chore: compact dependabot bumps" --body "Consolidates open Dependabot PRs into a single change." --base main
    ```

    Capture the new PR URL from the output.

11. **Close the Dependabot PRs** — for each Dependabot PR that was included, add a comment linking to the new PR, then close it:

    ```bash
    gh pr comment <number> --body "Superseded by <new-pr-url>"
    gh pr close <number>
    ```

12. **Clean up** — delete the patch file:

    ```bash
    rm dependabot-diffs.patch
    ```
