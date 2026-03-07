# Dockerfile for FleaMarket‑AI (host mode optional)
FROM python:3.11-slim

# Install system deps (git for repo access, ca‑certificates for https)
RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates && rm -rf /var/lib/apt/lists/*

# Create app user
RUN useradd -m appuser
WORKDIR /app

# Copy requirements & install
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ ./src/
COPY config.yaml ./
COPY README.md ./

# Create logs and db directories with correct permissions
RUN mkdir -p logs db && chown -R appuser:appuser /app

USER appuser

# Entrypoint runs the main loop
CMD ["python", "-m", "src.main"]