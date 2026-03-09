FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY src/ ./src/

# Run from /app so that python -m src.main works
ENV PYTHONPATH=/app
ENTRYPOINT ["python", "-m", "src.main"]
