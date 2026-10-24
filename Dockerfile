FROM python:3.11-slim-bookworm@sha256:f06d01d4ba3f8ad5af45a3ec87df19c1d317d8e8961edc0fabda61811af6f7cf

COPY --from=ghcr.io/astral-sh/uv:0.11.3@sha256:90bbb3c16635e9627f49eec6539f956d70746c409209041800a0280b93152823 /uv /usr/local/bin/uv

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

COPY pyproject.toml uv.lock ./
COPY .dlt ./.dlt

RUN uv sync --frozen --no-dev --no-install-project

COPY dlt_framework ./dlt_framework

RUN uv sync --frozen --no-dev

USER dlt
VOLUME ["/var/lib/dlt"]
ENTRYPOINT ["/usr/bin/tini", "--", "/app/.venv/bin/pipeline"]
