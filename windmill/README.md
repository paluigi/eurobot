# eurobot — Windmill flow

The same euro-area reporting pipeline, re-arranged as a [Windmill](https://www.windmill.dev) flow. The classic single-container pipeline (`python -m eurobot.scheduler`) remains fully supported; this folder runs the same logic as four discrete Windmill scripts with two databases:

- **PostgreSQL** — data layer: series registry, observations, news items, stats, dedup state, run log (`schema.sql`)
- **MongoDB** — inter-step handoff (`flow_state` helper collection, one doc per run) + the persistent post archive (`posts` collection, final JSON of every published post)

The helper collection is dropped by the final step once the post is published and archived — the handoff data never outlives a successful flow.

## Flow steps

| # | Script | Reads | Writes |
|---|--------|-------|--------|
| 1 | `fetch_data.py` | ECB/Eurostat SDMX, Yahoo Finance, RSS | PG: `series`, `observations`, `news_items`, `run_log` · Mongo: `flow_state` doc created |
| 2 | `compute_stats.py` | PG observations + Mongo `flow_state` | PG: `stats` · Mongo: summaries, fresh news/tags, charts, tables |
| 3 | `llm_report.py` | Mongo `flow_state` | Mongo: assembled payload (3-stage LLM: select → draft → review) |
| 4 | `publish_archive.py` | Mongo `flow_state` | zzboard POST → Mongo: `posts` archive · PG: dedup marks · **drops `flow_state`** |

Scripts take explicit arguments (`postgres_dsn`, `mongo_uri`, `run_id`, …) — no ambient config — and return JSON-serialisable results. All network calls use bounded tenacity retries (capped attempts, exponential backoff); the LLM stages resume from the last valid stage on retry.

## LLM cascade — dict config (llm-pycascade ≥ 0.2.0)

Step 3 configures the cascade via `config_from_dict()` (new in llm-pycascade 0.2.0) with a `:memory:` database — no TOML file or persistent state on the ephemeral Windmill worker. The dict is a Windmill variable with exactly the TOML schema:

```json
{
  "providers": {
    "groq":     { "type": "openai", "api_key_env": "GROQ_API_KEY", "base_url": "https://api.groq.com/openai/v1" },
    "together": { "type": "openai", "api_key_env": "TOGETHER_API_KEY", "base_url": "https://api.together.xyz/v1" }
  },
  "cascades": {
    "default": {
      "entries": [
        { "provider": "together", "model": "deepseek-ai/DeepSeek-V4-Flash-0731" },
        { "provider": "groq", "model": "qwen/qwen3.6-27b" }
      ]
    }
  },
  "database": { "path": ":memory:" },
  "failure_persistence": { "dir": "/tmp/llm-pycascade/failed_prompts" }
}
```

`api_key` may also be given inline with `"api_key_literal": true` when a secrets manager injects the key value at runtime (stored masked as `SecretStr`).

## Deploying to Windmill

```bash
# 1. Sync the scripts (adjust paths/worker tag to your workspace)
wmill sync add windmill --workspace-path u/eurobot --skip-dependencies
wmill sync push

# 2. Create the variables (Settings → Variables, or CLI)
wmill variable create u/eurobot/postgres_dsn --value 'postgres://user:***@host:5432/eurobot' --secret
wmill variable create u/eurobot/mongo_uri --value 'mongodb://user:***@host:27017' --secret
wmill variable create u/eurobot/zzboard_token --value '***' --secret
# cascade_config is a JSON variable — create it in the UI, or:
#   wmill variable create u/eurobot/cascade_config --json @cascade_config.example.json

# 3. Create the flow from flow.yaml
wmill flow create u/eurobot/daily_post --path windmill/flow.yaml
# (or Flow editor → New Flow → Edit in YAML → paste flow.yaml contents)

# 4. Schedule it (Schedules → New): 08:00, 13:00, 18:00 UTC
```

The Python worker needs the eurobot package (fetchers/stats/viz/prompts) plus its dependencies. Either bake them into a custom worker image, or keep the default worker and let the scripts install them at startup — simplest is a custom image based on `ghcr.io/windmill-labs/windmill-worker-python3` with `uv pip install eurobot llm-pycascade sdmx1 pandas plotly feedparser asyncpg pymongo tenacity apscheduler` (the exact set is in `pyproject.toml`).

## Local E2E test

`tests/test_windmill_e2e.py` runs the whole flow locally against throwaway PostgreSQL + MongoDB containers (`dry_run` publish, fake LLM responses). See the repo README for details.

## Notes

- **Dedup marking stays post-publish** — news/macro items are marked seen in PG only after the zzboard POST succeeds, so a failed flow re-presents the same content next run (same semantics as the classic pipeline).
- **`flow_state` is ephemeral by design** — if a run fails mid-flow the doc persists for debugging; the next successful run drops the collection. `fetch_data` recreates it.
- **Charts/tables are stored as JSON** in Mongo (Plotly specs), ready for the payload — no binary blobs.
