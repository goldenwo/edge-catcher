#!/bin/bash
# Start FastAPI backend + Vite dev server (dev mode)
cd "$(dirname "$0")"
pip install -e ".[ui]" -q
cd ui && npm install -q && npm run build && cd ..
uvicorn api.main:app --host 0.0.0.0 --port 8000
