FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY fetcher/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY fetcher/src/ ./src/
COPY scripts/init-db.sql ./scripts/init-db.sql

# Run from /app so that python -m src.main works
ENV PYTHONPATH=/app
ENTRYPOINT ["python", "-m", "src.main"]
