# Genie ZeroOps POC

A small medallion pipeline (synthetic orders → bronze → silver → gold) deployed as a
Databricks Asset Bundle, plus scripted "break" scenarios for evaluating Genie ZeroOps.
All data is synthetic.

## Layout
| Path | Purpose |
|---|---|
| `databricks.yml` | Bundle: pipeline, job, variables, `dev` target |
| `src/generate_orders.py` | Notebook: creates schema/volume if missing, writes ~500 fake JSON orders per run |
| `src/orders_pipeline.py` | Declarative pipeline (`pyspark.pipelines`): bronze → silver → gold |
| `scenarios/*.patch` | Five break scenarios (never applied on `main`) |
| `scripts/` | `apply_scenario.sh`, `reset_baseline.sh`, `status.sh` |

## Resources (dev target, development mode adds a `[dev <user>]` name prefix)
- Catalog `workspace`, schema `workspace.zeroops_poc`, volume `landing`
- Landing path `/Volumes/workspace/zeroops_poc/landing/orders`
- Tables: `bronze_orders`, `silver_orders`, `gold_daily_revenue`
- Job `zeroops_orders_job` (generate_orders → run_pipeline), hourly, Asia/Kolkata, **PAUSED**
- Pipeline `zeroops_orders_pipeline` (serverless)

## Setup
```bash
databricks auth login --host https://dbc-4747634d-f810.cloud.databricks.com --profile dev
databricks bundle validate -t dev -p dev
databricks bundle deploy   -t dev -p dev     # only from committed + pushed code
databricks bundle run zeroops_orders_job -t dev -p dev
./scripts/status.sh
```
Scripts use profile `dev` by default; override with `PROFILE=...`.
Schema and volume are created idempotently by the generator notebook.

## Rules
- **Never commit or push to `main` after the baseline.** Scenarios live on `scenario-N` branches.
- **Protect `main` in your Git provider** (GitHub: Settings → Branches → add rule; require PRs).
- Deploy only committed and pushed code so ZeroOps can map failures to commits.

## Scenarios
Run `./scripts/apply_scenario.sh N`: creates branch `scenario-N` from `origin/main`, applies the
patch, commits, pushes, deploys from that branch and triggers a run.
Reset with `./scripts/reset_baseline.sh` (add `--purge-landing` after scenario 2 and 4; it asks before deleting).

| # | Scenario | Behaviour | Expected ZeroOps result |
|---|---|---|---|
| 1 | Code bug | silver renames `country` → `customer_country`, gold still uses `country` | Loud failure. Root cause: **code change** (the commit on `scenario-1`) |
| 2 | Upstream schema change | generator emits `total_amount` as "12.50 INR" instead of `order_amount`; bronze uses `failOnNewColumns` | Bronze fails. Root cause: **schema change** in upstream data |
| 3 | Silent data loss | silver adds `.dropna()` with no subset, dropping ~75% of rows (null `promo_code`); job stays green | **Completeness anomaly** traced to the commit. Needs 2-3 days of baseline first |
| 4 | Stale table | generator writes 0 rows; pipeline still succeeds | **Freshness anomaly**. Needs baseline history first |
| 5 | Type overflow | silver casts `order_amount` to tinyint → ANSI cast error | Detected as failure; **judge the fix quality** (should restore decimal(12,2), not widen blindly) |

## Manual checklist (ZeroOps is configured in the UI)
- [ ] Admin: Settings → Previews → turn on **Genie ZeroOps**
- [ ] User Settings → Git integration: add your Git credential
- [ ] Catalog Explorer → `zeroops_poc` schema → turn on **anomaly detection** (data quality monitoring)
- [ ] Configure ZeroOps scope: the job, the pipeline, all tables in the schema; mark `gold_daily_revenue` **critical**
- [ ] Unpause the job schedule and let it run 2-3 days before scenarios 3 and 4
- [ ] Protect `main` in your Git provider

### Follow-up prompts to ask ZeroOps per insight
- What is the root cause, and which commit or upstream change introduced it?
- What downstream tables and consumers are affected?
- Propose a fix as a branch/PR and explain why it is safe.
- How would we detect this earlier next time?
- Is anything else in this schema degraded because of the same cause?

### Results template
| Scenario | Detected? | Time to insight | Root cause correct? | Fix correct? | Notes |
|---|---|---|---|---|---|
| 1 Code bug | | | | | |
| 2 Schema change | | | | | |
| 3 Silent dropna | | | | | |
| 4 Stale table | | | | | |
| 5 Type overflow | | | | | |
