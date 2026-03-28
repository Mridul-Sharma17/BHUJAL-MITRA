# Databricks Free Edition + CLI Official Reference (Hackathon Support Pack)

Compiled for: BHUJAL-MITRA (Swatantra track)

Scope:
- Databricks on AWS docs only (normal Databricks, non-Azure)
- Databricks Free Edition constraints
- Databricks CLI setup + usage
- Mappings to your hackathon mandatory requirements

Compiled on: 2026-03-28

---

## 1) What this document gives you

This is a single-file, offline-friendly reference so you do not have to repeatedly search the web during the hackathon.

It includes:
- Official setup and auth paths for Databricks CLI
- Practical command flow for local development and demos
- Official platform docs needed to satisfy judging criteria
- Free Edition constraints and how they affect architecture choices
- A reproducibility checklist for judge-friendly submissions

---

## 2) Official source index (validated)

### CLI core
- What is the Databricks CLI: https://docs.databricks.com/aws/en/dev-tools/cli/
- Install or update CLI: https://docs.databricks.com/aws/en/dev-tools/cli/install
- CLI authentication: https://docs.databricks.com/aws/en/dev-tools/cli/authentication
- CLI profiles: https://docs.databricks.com/aws/en/dev-tools/cli/profiles
- CLI basic usage: https://docs.databricks.com/aws/en/dev-tools/cli/usage
- CLI commands reference: https://docs.databricks.com/aws/en/dev-tools/cli/commands
- CLI tutorial: https://docs.databricks.com/aws/en/dev-tools/cli/tutorial
- CLI migration (legacy -> new): https://docs.databricks.com/aws/en/dev-tools/cli/migrate

### Auth (unified auth)
- Auth overview: https://docs.databricks.com/aws/en/dev-tools/auth/
- Unified auth: https://docs.databricks.com/aws/en/dev-tools/auth/unified-auth
- Env vars and fields: https://docs.databricks.com/aws/en/dev-tools/auth/env-vars

### Automation / CI-CD
- Declarative Automation Bundles (DAB): https://docs.databricks.com/aws/en/dev-tools/bundles/
- DAB configuration: https://docs.databricks.com/aws/en/dev-tools/bundles/settings
- DAB resources: https://docs.databricks.com/aws/en/dev-tools/bundles/resources/
- CI/CD on Databricks: https://docs.databricks.com/aws/en/dev-tools/ci-cd/

### User-facing app and notebook UI
- Databricks Apps: https://docs.databricks.com/aws/en/dev-tools/databricks-apps/
- Databricks widgets: https://docs.databricks.com/aws/en/notebooks/widgets
- Databricks web terminal: https://docs.databricks.com/aws/en/compute/web-terminal

### Platform docs for mandatory requirements
- Data lakehouse overview: https://docs.databricks.com/aws/en/lakehouse/
- Delta Lake overview: https://docs.databricks.com/aws/en/delta/
- AI/ML overview: https://docs.databricks.com/aws/en/machine-learning/
- Model Serving: https://docs.databricks.com/aws/en/machine-learning/model-serving/
- Lakeflow Jobs: https://docs.databricks.com/aws/en/jobs/

### Free Edition
- Sign up / Free Edition overview: https://docs.databricks.com/aws/en/getting-started/free-edition
- Free Edition limitations: https://docs.databricks.com/aws/en/getting-started/free-edition-limitations

### Official CLI GitHub
- Databricks CLI repo: https://github.com/databricks/cli
- CLI releases: https://github.com/databricks/cli/releases
- CLI README: https://raw.githubusercontent.com/databricks/cli/main/README.md

---

## 3) Databricks CLI setup (non-Azure, Free Edition compatible)

From official install docs:
- Applies to CLI versions 0.205+
- CLI is in Public Preview
- Linux/macOS install options: Homebrew, curl, source build

### Recommended install on Linux (quickest)

```bash
brew tap databricks/tap
brew install databricks
databricks version
```

### Alternative install via official script

```bash
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
databricks version
```

### Version check

```bash
databricks -v
databricks version
```

Target from docs: version should be 0.205.0 or above.

---

## 4) Authentication deep dive (for normal Databricks)

From official CLI/auth docs:
- Recommended interactive developer flow: OAuth U2M (user-to-machine)
- Recommended automation flow: OAuth M2M (service principal)
- PAT auth is legacy/deprecated in docs context

### 4.1 Workspace-level OAuth login (what you usually need in hackathon)

```bash
databricks auth login --host <workspace-url>
```

Example workspace host format from docs:
- https://dbc-a1b2345c-d6e7.cloud.databricks.com

### 4.2 Account-level OAuth login (optional; often not available in Free Edition)

```bash
databricks auth login --host https://accounts.cloud.databricks.com --account-id <account-id>
```

### 4.3 Profile inspection and token checks

```bash
databricks auth profiles
databricks auth env --profile <profile-name>
databricks auth token -p <profile-name>
```

### 4.4 .databrickscfg behavior

From official profile docs:
- Default file path:
  - Linux/macOS: ~/.databrickscfg
  - Windows: %USERPROFILE%/.databrickscfg
- Override path with:
  - DATABRICKS_CONFIG_FILE
- Choose default profile with:
  - DATABRICKS_CONFIG_PROFILE

### 4.5 Important environment variables (unified auth)

Core variables from official env-vars page:
- DATABRICKS_HOST
- DATABRICKS_TOKEN
- DATABRICKS_ACCOUNT_ID (for account endpoint operations)
- DATABRICKS_CLIENT_ID
- DATABRICKS_CLIENT_SECRET
- DATABRICKS_AUTH_TYPE
- DATABRICKS_CONFIG_FILE
- DATABRICKS_CONFIG_PROFILE

### 4.6 Authentication order notes (from CLI auth docs)

The docs note precedence behavior such as:
- Environment variables (for example DATABRICKS_TOKEN) can take precedence over profile values.
- CLI can resolve profile context via host and bundle settings.

Practical rule:
- Keep one clean profile for hackathon workspace.
- Avoid exporting stale DATABRICKS_TOKEN in your shell when using OAuth login profiles.

---

## 5) CLI quickstart flow you can run anytime

```bash
# 1) Confirm CLI
 databricks version

# 2) Login to workspace
 databricks auth login --host <your-workspace-url>

# 3) Check auth profiles
 databricks auth profiles

# 4) Validate current identity
 databricks current-user me

# 5) Explore command groups
 databricks -h

# 6) Explore specific command help
 databricks jobs -h
 databricks workspace -h
 databricks bundle -h
```

If running inside Databricks web terminal:
- Docs say latest CLI is available there.
- Profile commands are not supported there (auth comes from environment in web terminal context).

---

## 6) CLI command groups you should know first

From official command reference page, important groups for hackathon work include:
- workspace
- clusters
- jobs
- pipelines
- bundle
- apps
- current-user
- auth
- configure
- fs
- sql
- serving-endpoints
- vector-search-endpoints
- experiments / model registry related groups
- api (raw REST calls)
- version

Universal flags (from command docs):
- -p, --profile <name>
- -o, --output <type>
- -t, --target <bundle-target>
- -h, --help

---

## 7) Declarative Automation Bundles (DAB) for reproducible demos

Official docs position DAB as the recommended IaC/CI-CD style for Databricks projects.

Why this matters for judging:
- Judges try to reproduce your demo.
- DAB lets you version project resources (jobs, pipelines, apps, serving endpoints, etc.).
- You can validate/deploy/run from CLI in repeatable steps.

### Typical DAB lifecycle commands

```bash
databricks bundle init
databricks bundle validate
databricks bundle deploy
databricks bundle run <resource-name>
```

Official docs also highlight:
- DAB can define jobs, pipelines, dashboards, model serving endpoints, MLflow resources, apps.
- DAB resources docs include Streamlit app template examples.
- CI/CD docs explicitly recommend DAB for Databricks CI/CD workflows.

---

## 8) Mandatory requirement mapping (hackathon rubric -> official Databricks docs)

### Requirement 1: Databricks as core

What to implement:
- Delta tables for your groundwater and rainfall datasets.
- Spark-based transformations/feature engineering.
- Lakehouse layering and governed assets.
- Job/pipeline orchestration for repeatability.

Official docs:
- Lakehouse: https://docs.databricks.com/aws/en/lakehouse/
- Delta Lake: https://docs.databricks.com/aws/en/delta/
- Lakeflow Jobs: https://docs.databricks.com/aws/en/jobs/
- DAB: https://docs.databricks.com/aws/en/dev-tools/bundles/

### Requirement 2: AI must be central

What to implement:
- ML forecasting pipeline for groundwater levels.
- RAG or model inference layer for advisory responses.
- Observable model lifecycle and serving/testing path.

Official docs:
- AI/ML overview: https://docs.databricks.com/aws/en/machine-learning/
- Model Serving: https://docs.databricks.com/aws/en/machine-learning/model-serving/

### Requirement 3: Prefer models made in India

Hackathon policy preference (not a strict Databricks requirement):
- Use IndicTrans2 / Airavata / Param / Sarvam where possible.

Databricks-aligned implementation path:
- Integrate your selected model in notebook/serving/app flow.
- Use Model Serving (where available) or in-notebook inference based on Free Edition limits.
- Keep model choice and reasoning explicit in README and demo.

Relevant docs:
- AI/ML overview: https://docs.databricks.com/aws/en/machine-learning/
- Model Serving: https://docs.databricks.com/aws/en/machine-learning/model-serving/

### Requirement 4: Working demo required

What to implement:
- One-command or few-command runbook.
- Deterministic job/app entry points.
- DAB-based deployment and job execution for reproducibility.

Official docs:
- CLI usage: https://docs.databricks.com/aws/en/dev-tools/cli/usage
- CLI commands: https://docs.databricks.com/aws/en/dev-tools/cli/commands
- DAB + CI/CD: https://docs.databricks.com/aws/en/dev-tools/bundles/
- CI/CD overview: https://docs.databricks.com/aws/en/dev-tools/ci-cd/

### Requirement 5: User-facing component (App / Notebook UI / etc.)

What to implement:
- Databricks App (best for polished demo) OR
- Notebook widgets + dashboard style flow (fastest fallback)

Official docs:
- Databricks Apps: https://docs.databricks.com/aws/en/dev-tools/databricks-apps/
- Widgets: https://docs.databricks.com/aws/en/notebooks/widgets

---

## 9) Free Edition constraints you must design around

From official Free Edition limitations page:

### Compute and platform
- Serverless-only environment
- Custom compute configs are not supported
- GPUs are not supported
- Outbound internet access is restricted to trusted domains

### Key quotas/limits called out in docs
- SQL warehouse: 1 warehouse, limited to 2X-Small
- Jobs: max 5 concurrent job tasks per account
- Model serving: capped active endpoints, no GPU endpoints, no custom models on GPU or batch inference
- Vector Search: 1 endpoint, 1 vector search unit, Direct Vector Access not supported
- Apps: 1 Databricks App per account; auto-stop after up to 24h after start/update/redeploy

### Administrative limits
- 1 workspace + 1 metastore per account
- No account console / account-level APIs
- No SSO/SCIM
- Non-commercial use

### Operational note
- If you exceed quota, compute can be shut down for the rest of the day (or longer in extreme cases), while data/settings remain.

Official references:
- Free Edition overview: https://docs.databricks.com/aws/en/getting-started/free-edition
- Free Edition limitations: https://docs.databricks.com/aws/en/getting-started/free-edition-limitations

---

## 10) What these Free Edition limits mean for Bhujal-Mitra

### Architecture choices that fit limits
- Use Spark + Delta for tabular forecasting data (CPU-friendly).
- Keep model sizes small and CPU-suitable for inference.
- Prefer batch scoring and cached outputs where practical.
- Keep jobs under concurrency limits (sequence tasks where possible).
- Treat app as single polished interface (1 app/account limit).
- Keep fallback UI in notebooks with widgets in case app fails.

### Demo safety strategy
- Prepare one stable demo path and one fallback path.
- Pre-run key transformations before final judging window.
- Keep a short "demo runbook" with exact commands and expected outputs.

---

## 11) Reproducibility checklist for judging day

### Before code freeze
- CLI version confirmed (0.205+)
- Auth profile works (`databricks current-user me`)
- Data ingestion pipeline rerun successfully
- Forecast job rerun successfully
- Advisory app/notebook UI works end-to-end
- README has exact run commands
- Architecture diagram included

### Submission integrity
- Public repo link valid
- Minimal setup steps clear
- Demo video <= 2 min and matches current code
- Deployed prototype link alive

### Emergency fallback
- Notebook with widgets can reproduce core workflow
- Precomputed forecast table available if model step is slow
- Backup script/commands ready for quick rerun

---

## 12) Minimal command cheat sheet

### Install / verify

```bash
brew tap databricks/tap
brew install databricks
databricks version
```

### Login / profiles

```bash
databricks auth login --host <workspace-url>
databricks auth profiles
databricks auth env --profile <profile-name>
databricks auth token -p <profile-name>
databricks current-user me
```

### Discover commands

```bash
databricks -h
databricks jobs -h
databricks workspace -h
databricks bundle -h
databricks apps -h
```

### Bundle lifecycle

```bash
databricks bundle init
databricks bundle validate
databricks bundle deploy
databricks bundle run <resource-name>
```

---

## 13) Suggested next docs to add later (if needed)

- Specific command pages you use most (for example jobs create/run-now, workspace import/export, apps deploy).
- Lakeflow pipeline pages used by your exact data workflow.
- Model/evaluation docs tied to your chosen Indian-language model stack.

---

## 14) Notes on terminology updates

From current official CLI release notes/changelog context:
- Databricks Asset Bundles are now called Declarative Automation Bundles (DAB).
- Existing configurations are described as non-breaking in release notes context.

Reference:
- https://github.com/databricks/cli/releases

---

## 15) One-line implementation strategy for your hackathon

Use Delta + Spark + Jobs for reliable data/forecast pipeline, then expose a user-facing advisory through Databricks App (or notebook widgets fallback), and automate deployment with CLI + DAB so judges can reproduce quickly.
