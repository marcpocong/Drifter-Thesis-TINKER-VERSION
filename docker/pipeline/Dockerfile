FROM python:3.10-slim

LABEL maintainer="kkreiju"
LABEL description="Phase 1: Ingestion & Validation"

# System Dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    git \
    libeccodes-dev \
    libnetcdf-dev \
    libhdf5-dev \
    libproj-dev \
    libgeos-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Poetry
RUN pip install --no-cache-dir poetry==1.7.0

WORKDIR /app

# Copy Phase 1 Config
COPY docker/pipeline/pyproject.toml pyproject.toml
# We don't copy poetry.lock because we want to resolve fresh for this new file

# Install Dependencies
RUN poetry config virtualenvs.in-project true && \
    poetry install --no-interaction --no-ansi

ENV PATH="/app/.venv/bin:$PATH"

COPY . .

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

CMD ["python", "-m", "src"]
