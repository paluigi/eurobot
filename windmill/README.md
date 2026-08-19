# eurobot — Windmill flow

The same euro-area reporting pipeline, re-arranged as a [Windmill](https://www.windmill.dev) flow. The classic single-container pipeline (`python -m eurobot.scheduler`) remains fully supported; this folder runs the same logic as four discrete Windmill scripts with two databases:

- **PostgreSQL** — data layer: series registry, observations, news items, stats, dedup state, run log (schema embedded in each script)
- **MongoDB** — inter-step handoff (`flow_state` helper collection, one doc per run) + the persistent post archive (`posts` collection, final JSON of every published post)

The helper collection is dropped by the final step once the post is published and archived — the handoff data never outlives a successful flow.

Every step script is **fully self-contained**: the PostgreSQL schema (`SCHEMA_SQL`), the Mongo handoff helpers (`FlowStore`, `pg_connect`, `mongo_client`) and all domain code (fetchers, stats, chart builders, LLM prompts) are defined inside each script — there is no eurobot package to install, no shared module and no external SQL/config file, so any script can be pasted into Windmill and run standalone. The only dependencies are published PyPI libraries.

## Flow steps

| # | Script | Reads | Writes |
|---|--------|-------|--------|
| 1 | `fetch_data.py` | ECB/Eurostat SDMX, Yahoo Finance, RSS | PG: `series`, `observations`, `news_items`, `run_log` · Mongo: `flow_state` doc created |
| 2 | `compute_stats.py` | PG observations + Mongo `flow_state` | PG: `stats` · Mongo: summaries, fresh news/tags, charts, tables |
| 3 | `llm_report.py` | Mongo `flow_state` | Mongo: assembled payload (3-stage LLM: select → draft → review) |
| 4 | `publish_archive.py` | Mongo `flow_state` | zzboard POST → Mongo: `posts` archive · PG: dedup marks · **drops `flow_state`** |

Scripts take explicit arguments (`postgres_dsn`, `mongo_uri`, `run_id`, …) — no ambient config — and return JSON-serialisable results. All network calls use bounded tenacity retries (capped attempts, exponential backoff); the LLM stages resume from the last valid stage on retry.

## LLM cascade — config defined in `llm_report.py` (llm-pycascade ≥ 0.2.0)

Step 3 configures the cascade via `config_from_dict()` with a `:memory:` database — no TOML file or persistent state on the ephemeral Windmill worker. The config is **built inside the script** from two editable tables at the top of `llm_report.py`:

```python
PROVIDERS = {
    "groq":     {"base_url": "https://api.groq.com/openai/v1",
                 "api_key_variable": "u/eurobot/groq_api_key"},
    "together": {"base_url": "https://api.together.xyz/v1",
                 "api_key_variable": "u/eurobot/together_api_key"},
}
CASCADE_ENTRIES = [
    {"provider": "together", "model": "deepseek-ai/DeepSeek-V4-Flash-0731"},
    {"provider": "together", "model": "Prism-ML/Ternary-Bonsai-27B"},
    {"provider": "groq", "model": "qwen/qwen3.6-27b"},
    {"provider": "groq", "model": "openai/gpt-oss-120b"},
]
```

API keys are Windmill **secret variables**, fetched at runtime with `wmill.get_variable()` and injected with `"api_key_literal": true` (stored masked as `SecretStr` — no environment variables on the worker, and no key arguments in the flow wiring). To add a provider: create its secret variable, add one `PROVIDERS` entry pointing at it, and reference it from `CASCADE_ENTRIES`. To change the model order, edit `CASCADE_ENTRIES` — first entry is tried first.

## Deploying to Windmill

```bash
# 1. Sync the scripts (adjust paths/worker tag to your workspace)
wmill sync add windmill --workspace-path u/eurobot --skip-dependencies
wmill sync push

# 2. Create the variables (Settings → Variables, or CLI)
wmill variable create u/eurobot/postgres_dsn --value 'postgres://user:***@host:5432/eurobot' --secret
wmill variable create u/eurobot/mongo_uri --value 'mongodb://user:***@host:27017' --secret
wmill variable create u/eurobot/zzboard_token --value '***' --secret
wmill variable create u/eurobot/groq_api_key --value '***' --secret
wmill variable create u/eurobot/together_api_key --value '***' --secret

# 3. Create the flow from flow.yaml
wmill flow create u/eurobot/daily_post --path windmill/flow.yaml
# (or Flow editor → New Flow → Edit in YAML → paste flow.yaml contents)

# 4. Schedule it (Schedules → New): 08:00, 13:00, 18:00 UTC
```

The Python worker does **not** need the eurobot package — the scripts embed all domain code and only depend on published PyPI libraries. Either bake them into a custom worker image, or keep the default worker and let the scripts install them at startup — simplest is a custom image based on `ghcr.io/windmill-labs/windmill-worker-python3` with `uv pip install llm-pycascade sdmx1 pandas plotly feedparser asyncpg pymongo requests tenacity`.

## Local E2E test

`tests/test_windmill_e2e.py` runs the whole flow locally against throwaway PostgreSQL + MongoDB containers (`dry_run` publish, fake LLM responses). See the repo README for details.

## Notes

- **Dedup marking stays post-publish** — news/macro items are marked seen in PG only after the zzboard POST succeeds, so a failed flow re-presents the same content next run (same semantics as the classic pipeline).
- **`flow_state` is ephemeral by design** — if a run fails mid-flow the doc persists for debugging; the next successful run drops the collection. `fetch_data` recreates it.
- **Charts/tables are stored as JSON** in Mongo (Plotly specs), ready for the payload — no binary blobs.
