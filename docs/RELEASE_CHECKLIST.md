# Release Guide: microsoft/dbt-fabricspark

## 📋 Checklist

| #   | Task                                    | Where                                         |
| --- | --------------------------------------- | --------------------------------------------- |
| 1   | Pick new version number                 | Your head 🧠                                  |
| 2   | Bump `__version__.py`                   | `src/dbt/adapters/fabricspark/__version__.py` |
| 3   | Update `CHANGELOG.md`                   | `CHANGELOG.md`                                |
| 4   | Push to `main`, confirm CI is green     | GitHub Actions → `ci.yml`                     |
| 5   | Create GitHub Release with tag `vX.Y.Z` | github.com/…/releases/new                     |
| 6   | Watch `release.yml` pipeline            | GitHub Actions → `release.yml`                |
| 7   | Verify on PyPI                          | pypi.org/project/dbt-fabricspark              |

## 🗺️ How a Release Works (Big Picture)

When you create a **GitHub Release** with a tag like `v1.9.6`, it automatically triggers the `release.yml` workflow, which:

1. Builds the Python package
2. Publishes the package to **PyPI** automatically

---

## Step-by-Step Release Plan

### Step 1 — Decide the new version number

This project uses **semantic versioning** (`vMAJOR.MINOR.PATCH`):

- **PATCH** (`v1.9.6`): Bug fixes only
- **MINOR** (`v1.10.0`): New features, backwards-compatible
- **MAJOR** (`v2.0.0`): Breaking changes

👉 For most releases, you'll increment the **PATCH** number (e.g. `1.9.5` → `1.9.6`).

---

### Step 2 — Bump the version in code

The single source of truth for the version is `src/dbt/adapters/fabricspark/__version__.py`:

Edit this file and change `"1.9.5"` to your new version (e.g. `"1.9.6"`). This is the only file you need to change — `pyproject.toml` reads it dynamically.

Commit this change:

```sh
git commit -am "chore: bump version to v1.9.6"
git push origin main
```

---

### Step 3 — Update `CHANGELOG.md`

Open `CHANGELOG.md` and add a new section **at the top** (above `## v1.9.5`), following the same format already used:

```
## v1.9.6

### Bug Fixes

#### Short title describing the fix

**Problem:** What was broken and why.

**Fix:** What you changed to fix it.
```

Then commit and push:

```sh
git commit -am "docs: update CHANGELOG for v1.9.6"
git push origin main
```

---

### Step 4 — Make sure `main` is clean and green

Before releasing, verify the CI workflow is passing on `main`:

1. Go to **Actions → ci.yml** on GitHub
2. Confirm the latest run on `main` is ✅ green (ruff linting, unit tests across Python 3.9–3.13, build verification)

**Do not release from a broken `main`.**

---

### Step 5 — Create a GitHub Release (this triggers the publish)

This is the magic step. Creating a release with the right tag automatically fires the release pipeline.

1. Go to `github.com/microsoft/dbt-fabricspark/releases/new`
2. In **"Choose a tag"**, type `v1.9.6` and click **"Create new tag: v1.9.6 on publish"**
3. Set **Target** to `main`
4. Set the **Release title** to `v1.9.6`
5. In the **description box**, paste your changelog content from Step 3 (or click "Generate release notes" to auto-populate from PR titles)
6. Leave **"Set as the latest release"** checked
7. Click **"Publish release"** 🚀

---

### Step 6 — Watch the release pipeline

1. Go to **Actions → release.yml** on GitHub
2. You'll see a new run triggered by the tag. It will:
   - 📦 Build the wheel/sdist (`nx run dbt-fabricspark:build`)
   - 🚀 Publish to PyPI (`nx run dbt-fabricspark:publish`) using the `PYPI_TOKEN` secret
3. Monitor for ✅ or ❌.

---

### Step 7 — Verify the PyPI publish

Once the workflow is green:

1. Go to `pypi.org/project/dbt-fabricspark`
2. Confirm `v1.9.6` is listed as the latest version
3. Test it locally: `pip install dbt-fabricspark==1.9.6`

---

## 🔥 Removing a Tag (Aborting or Re-doing a Release)

If a release was created by mistake or you need to redo it:

### Delete the remote tag

```sh
git push origin --delete vX.Y.Z
```

### Delete the local tag (if it exists)

```sh
git tag -d vX.Y.Z
```

### Delete the GitHub Release

1. Go to `github.com/microsoft/dbt-fabricspark/releases`
2. Find the release, click **"Delete"**

> ⚠️ If the package was already published to PyPI, you **cannot** re-upload the same version. You'll need to bump to a new patch version (e.g. `v1.9.7`).
