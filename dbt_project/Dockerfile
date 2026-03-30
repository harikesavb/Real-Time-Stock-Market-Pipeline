FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV DBT_PROFILES_DIR=/app/dbt_project

WORKDIR /app/dbt_project

RUN apt-get update \
    && apt-get install --yes --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

COPY dbt_project/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY dbt_project /app/dbt_project

CMD ["sleep", "infinity"]
