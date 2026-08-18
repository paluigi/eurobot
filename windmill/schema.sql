-- eurobot Windmill flow — PostgreSQL schema (data layer)
--
-- Raw + structured data lives here: series registry, observations, news
-- items, dedup state and per-run stats/logs. Applied idempotently at the
-- start of every flow run (CREATE TABLE IF NOT EXISTS).

CREATE TABLE IF NOT EXISTS series (
    tag         TEXT PRIMARY KEY,
    title       TEXT NOT NULL,
    source      TEXT NOT NULL,               -- ECB | ESTAT | YAHOO | COMPUTED
    frequency   TEXT NOT NULL,               -- D | M | Q
    unit        TEXT NOT NULL,
    description TEXT,
    first_seen  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS observations (
    tag        TEXT NOT NULL REFERENCES series(tag) ON DELETE CASCADE,
    obs_date   DATE   NOT NULL,
    value      DOUBLE PRECISION NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tag, obs_date)
);

CREATE TABLE IF NOT EXISTS news_items (
    hash       TEXT PRIMARY KEY,             -- sha256(title|link)[:16]
    title      TEXT NOT NULL,
    summary    TEXT,
    link       TEXT NOT NULL,
    source     TEXT NOT NULL,
    first_seen TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Dedup state — written ONLY after a successful publish (publish_archive).
CREATE TABLE IF NOT EXISTS news_seen (
    hash         TEXT PRIMARY KEY,
    first_seen   TIMESTAMPTZ NOT NULL,
    times_posted INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS macro_releases (
    series_key         TEXT PRIMARY KEY,
    latest_obs_period  TEXT,
    release_timestamp  TIMESTAMPTZ,
    first_presented    TIMESTAMPTZ,
    value_hash         TEXT
);

CREATE TABLE IF NOT EXISTS run_log (
    run_id     UUID PRIMARY KEY,
    started_at TIMESTAMPTZ NOT NULL,
    step       TEXT NOT NULL,                -- fetch | stats | llm | publish | done
    status     TEXT NOT NULL,                -- running | succeeded | failed | skipped
    detail     TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS stats (
    run_id        UUID NOT NULL REFERENCES run_log(run_id) ON DELETE CASCADE,
    tag          TEXT NOT NULL,
    latest_value DOUBLE PRECISION,
    latest_date  DATE,
    delta_prev   DOUBLE PRECISION,
    delta_prev_pct DOUBLE PRECISION,
    yoy          DOUBLE PRECISION,
    yoy_pct      DOUBLE PRECISION,
    summary      TEXT,
    computed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, tag)
);

CREATE INDEX IF NOT EXISTS idx_observations_tag_date ON observations (tag, obs_date DESC);
CREATE INDEX IF NOT EXISTS idx_stats_run ON stats (run_id);
