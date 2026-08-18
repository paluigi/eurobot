# syntax=docker/dockerfile:1
# Reproducible-image notes:
#   - Base image pinned by digest (refresh: docker manifest inspect python:3.12-slim)
#   - Dependencies pinned by uv.lock (committed)
#   - All files enter via RUN + bind mounts and every layer's mtimes are
#     normalized to SOURCE_DATE_EPOCH: identical inputs → identical digest.
#   - Build with: scripts/build-image.sh (passes --build-arg SOURCE_DATE_EPOCH)
FROM python:3.12-slim@sha256:876416ecde9aca2bcc90e1fb0c7a9500bbf749f5788b70f82d4c5a5c2357f8b4

LABEL maintainer="Luigi Palumbo"
LABEL description="Autonomous euro-area economic reporting pipeline"
LABEL org.opencontainers.image.title="eurobot"
LABEL org.opencontainers.image.description="Autonomous euro-area economic reporting pipeline: fetches ECB/Eurostat, market and news data, drafts an LLM report and publishes it to a zzboard API"
LABEL org.opencontainers.image.source="https://github.com/paluigi/eurobot"
LABEL org.opencontainers.image.licenses="MIT"

# Fixed epoch for all file mtimes: layer digests depend only on file
# contents, never on the build clock.
ENV SOURCE_DATE_EPOCH=946684800

# Strip apt state and the random machine-id so the layer is deterministic.
# (No apt packages needed since supercronic was dropped for APScheduler.)
RUN rm -rf /var/lib/apt/lists/* /var/cache/apt/archives/*.deb \
    && rm -f /var/log/apt/* /var/log/dpkg.log /var/log/alternatives.log \
    && rm -f /var/cache/ldconfig/aux-cache \
    && : > /etc/machine-id \
    && mkdir -p /app \
    && find / -xdev -exec touch -h -d "@${SOURCE_DATE_EPOCH}" {} + 2>/dev/null || true

# /app already exists (created with a normalized mtime above) so WORKDIR
# does not add a build-clock-stamped layer of its own.
WORKDIR /app

# All remaining setup happens in a single RUN layer. Files enter via bind
# mounts instead of COPY (COPY layers stamp parent-directory mtimes with
# the build clock, which would make the digests non-reproducible).
# uv_cache.json and its RECORD entry embed build timestamps — drop both.
# Scheduling is APScheduler inside python -m eurobot.scheduler (no cron
# daemon needed since 0.2.0).
RUN --mount=type=bind,source=src,target=/ctx-src \
    --mount=type=bind,source=config,target=/ctx-config \
    --mount=type=bind,source=pyproject.toml,target=/ctx-pyproject.toml \
    --mount=type=bind,source=uv.lock,target=/ctx-uv.lock \
    --mount=type=bind,from=ghcr.io/astral-sh/uv:0.12.5@sha256:e85be844203885286c60ffad8a858d48afb6c5a5c237ca0e67f12e74b8f174b1,source=/uv,target=/uvbin \
    set -eux; \
    cp /uvbin /usr/local/bin/uv; \
    cp -a /ctx-src ./src; \
    cp -a /ctx-config ./config; \
    cp /ctx-pyproject.toml ./pyproject.toml; \
    cp /ctx-uv.lock ./uv.lock; \
    uv export --frozen --no-emit-project --format requirements-txt > /tmp/requirements.lock; \
    uv pip install --system --no-cache -r /tmp/requirements.lock; \
    uv pip install --system --no-cache --no-deps /app; \
    rm -f /tmp/requirements.lock; \
    find /usr/local/lib/python3.12/site-packages -name 'uv_cache.json' -delete; \
    find /usr/local/lib/python3.12/site-packages -name 'RECORD' -exec sed -i '/uv_cache\.json/d' {} +; \
    mkdir -p /app/data/posts; \
    find / -xdev -exec touch -h -d "@${SOURCE_DATE_EPOCH}" {} + 2>/dev/null || true

# Environment defaults (overridden by docker-compose env_file)
ENV EUROBOT_CONFIG_DIR=/app/config \
    EUROBOT_DATA_DIR=/app/data \
    PYTHONUNBUFFERED=1

# Volume mount points
VOLUME ["/app/config", "/app/data"]

# APScheduler entry point — daily runs at 08:00/13:00/18:00 UTC
# (override with SCHEDULE_HOURS; RUN_ON_START=1 for an immediate run)
CMD ["python", "-m", "eurobot.scheduler"]
