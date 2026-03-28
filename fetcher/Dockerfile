FROM python:3.12-slim

WORKDIR /app

ARG TA_LIB_VERSION=0.6.4

# Install TA-Lib native library and Python dependencies.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        fonts-dejavu-core \
        libcairo2 \
        libffi-dev \
        libgdk-pixbuf-2.0-0 \
        libharfbuzz0b \
        libharfbuzz-subset0 \
        libpango-1.0-0 \
        libpangoft2-1.0-0 \
        shared-mime-info \
        wget \
    && wget -q "https://github.com/TA-Lib/ta-lib/releases/download/v${TA_LIB_VERSION}/ta-lib-${TA_LIB_VERSION}-src.tar.gz" \
    && tar -xzf "ta-lib-${TA_LIB_VERSION}-src.tar.gz" \
    && cd "ta-lib-${TA_LIB_VERSION}" \
    && ./configure --prefix=/usr \
    && make \
    && make install \
    && cd /app \
    && rm -rf \
        "ta-lib-${TA_LIB_VERSION}" \
        "ta-lib-${TA_LIB_VERSION}-src.tar.gz" \
        /var/lib/apt/lists/*

COPY fetcher/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY fetcher/src/ ./src/
COPY scripts/init-db.sql ./scripts/init-db.sql

# Run from /app so that python -m src.main works
ENV PYTHONPATH=/app
ENTRYPOINT ["python", "-m", "src.main"]
