FROM python:3.11-bullseye

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml README.md ./
COPY pipeline ./pipeline

RUN pip install --no-cache-dir .

ENTRYPOINT ["python", "-m", "pipeline.execution.run_pipeline"]
