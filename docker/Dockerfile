# ─────────────────────────────────────────────────────────────────────
# IBKR Options — unified image (PySpark + Streamlit dashboard)
#
# Multi-stage build:
#   Stage 1 (base)      — Python deps shared by both targets
#   Stage 2 (app)       — Spark + ib_async for the pipeline
#   Stage 3 (dashboard) — lightweight Streamlit for viewing results
#
# docker-compose selects the target via `build.target`.
# ─────────────────────────────────────────────────────────────────────

# ═════════════════════════════════════════════════════════════════════
# Stage: app  — PySpark pipeline
# ═════════════════════════════════════════════════════════════════════
FROM apache/spark:4.1.1-scala2.13-java17-python3-ubuntu AS app

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt /opt/app/requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /opt/app/requirements.txt

COPY src/    /opt/app/src/
COPY config/ /opt/app/config/

WORKDIR /opt/app

ENV PYTHONPATH="/opt/app"
ENV OUTPUT_DIR="/opt/app/data"

ENTRYPOINT ["python", "/opt/app/src/main.py"]

# ═════════════════════════════════════════════════════════════════════
# Stage: dashboard  — Streamlit viewer (no Java / Spark needed)
# ═════════════════════════════════════════════════════════════════════
FROM python:3.12-slim AS dashboard

RUN apt-get update && \
    apt-get install -y --no-install-recommends && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt /opt/app/requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /opt/app/requirements.txt

COPY src/dashboard/ /opt/app/src/dashboard/
COPY config/         /opt/app/config/

WORKDIR /opt/app

ENV PYTHONPATH="/opt/app"

EXPOSE 8501

ENTRYPOINT ["python", "/opt/app/src/dashboard/run.py"]
