#!/bin/bash
cd "$(dirname "$0")"
uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload &
cd ui && npm run dev
