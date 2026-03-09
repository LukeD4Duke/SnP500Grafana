#!/bin/sh
# Entrypoint: run the fetcher (waits for DB internally)
set -e
exec python -m src.main
