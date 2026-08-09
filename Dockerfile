FROM python:3.11-slim-bookworm

COPY --from=ghcr.io/astral-sh/uv:0.11.3 /uv /usr/local/bin/uv

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DLT_DATA_DIR=/var/lib/dlt \
    LOG_FORMAT=json

RUN apt-get update \
    && apt-get install --yes --no-install-recommends tini \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system dlt \
    && useradd --system --gid dlt --home-dir /app dlt \
    && mkdir -p /var/lib/dlt /data \
    && chown -R dlt:dlt /var/lib/dlt /data

WORKDIR /app

COPY pyproject.toml uv.lock README.md ./
COPY .dlt ./.dlt

RUN uv sync --frozen --no-dev --no-install-project

COPY dlt_framework ./dlt_framework

RUN uv sync --frozen --no-dev

USER dlt
VOLUME ["/var/lib/dlt"]
ENTRYPOINT ["/usr/bin/tini", "--", "/app/.venv/bin/dlt-framework"]
