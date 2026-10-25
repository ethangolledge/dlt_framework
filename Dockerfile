FROM python:3.11-slim-bookworm@sha256:f06d01d4ba3f8ad5af45a3ec87df19c1d317d8e8961edc0fabda61811af6f7cf

COPY --from=ghcr.io/astral-sh/uv:0.11.3@sha256:90bbb3c16635e9627f49eec6539f956d70746c409209041800a0280b93152823 /uv /usr/local/bin/uv

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DLT_DATA_DIR=/var/lib/dlt \
    LOGGING__FORMAT=json

# System packages a pipeline needs, e.g. SYSTEM_PACKAGES="tesseract-ocr-eng" for OCR language data.
ARG SYSTEM_PACKAGES=""
RUN apt-get update \
    && apt-get install --yes --no-install-recommends tini $SYSTEM_PACKAGES \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system dlt \
    && useradd --system --gid dlt --home-dir /app dlt \
    && mkdir -p /var/lib/dlt /data \
    && chown -R dlt:dlt /var/lib/dlt /data

WORKDIR /app

# The extras this image needs: its destinations and source types, stated at build time, e.g.
#   docker build --build-arg EXTRAS="--extra postgres --extra sql" .
ARG EXTRAS
RUN test -n "$EXTRAS" || { echo 'State EXTRAS, e.g. --build-arg EXTRAS="--extra postgres"'; exit 1; }

COPY pyproject.toml uv.lock ./
COPY .dlt ./.dlt

RUN uv sync --frozen --no-dev --no-install-project $EXTRAS

COPY dlt_framework ./dlt_framework

RUN uv sync --frozen --no-dev $EXTRAS

USER dlt
VOLUME ["/var/lib/dlt"]
ENTRYPOINT ["/usr/bin/tini", "--", "/app/.venv/bin/pipeline"]
