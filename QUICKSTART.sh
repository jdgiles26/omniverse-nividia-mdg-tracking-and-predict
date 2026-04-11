#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${PROJECT_ROOT}"

if ! command -v docker >/dev/null 2>&1; then
  echo "ERROR: docker is not installed or not in PATH."
  exit 1
fi

if ! docker compose version >/dev/null 2>&1; then
  echo "ERROR: docker compose plugin is not available."
  exit 1
fi

if [[ ! -f ".env" ]]; then
  cp .env.example .env
  echo "Created .env from .env.example"
fi

if ! grep -q '^NVIDIA_API_KEY=' .env; then
  echo "WARN: NVIDIA_API_KEY is missing in .env. VSS/Cosmos NIM calls may fail."
fi

NVIDIA_API_KEY_VALUE="$(grep '^NVIDIA_API_KEY=' .env | tail -n 1 | cut -d'=' -f2- | xargs)"
if [[ -z "${NVIDIA_API_KEY_VALUE}" || "${NVIDIA_API_KEY_VALUE}" == "nvapi-your-key-here" || "${NVIDIA_API_KEY_VALUE}" == "nvapi-YOUR-KEY-HERE" ]]; then
  echo "WARN: NVIDIA_API_KEY in .env is placeholder/empty. VSS/Cosmos NIM calls may fail."
fi

echo "Starting Mission Data Grid stack..."
if docker info --format '{{json .Runtimes}}' | grep -q '"nvidia"'; then
  docker compose --profile gpu up -d --build
  echo "GPU profile enabled (DeepStream active)."
else
  docker compose up -d --build
  echo "GPU runtime not detected; started without DeepStream (CPU-safe mode)."
fi

echo
echo "Mission Data Grid is LIVE"
echo "Dashboard:   http://localhost:8080"
echo "Jupyter:     http://localhost:8888"
echo "VSS API:     http://localhost:8000/docs"
echo "Grid API:    http://localhost:8002/docs"
echo "Cosmos API:  http://localhost:8001/docs"
