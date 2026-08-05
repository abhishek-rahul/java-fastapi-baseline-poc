#!/bin/sh
set -eu

UVICORN_WORKERS="${1:-4}"

PYTHON_PROJECT_DIR="/app/python-fastapi"
UVICORN_PID=""

cleanup() {
    if [ -n "$UVICORN_PID" ] && kill -0 "$UVICORN_PID" 2>/dev/null; then
        kill -TERM "$UVICORN_PID" 2>/dev/null || true
        wait "$UVICORN_PID" 2>/dev/null || true
    fi
}

trap cleanup EXIT INT TERM

echo "Starting baseline TCP server: host=127.0.0.1, port=8000, uvicornWorkers=$UVICORN_WORKERS"
cd "$PYTHON_PROJECT_DIR"
python3 -m uvicorn app.main:app \
    --host 127.0.0.1 \
    --port 8000 \
    --workers "$UVICORN_WORKERS" \
    --no-access-log &
UVICORN_PID=$!

READY=0
ATTEMPT=0
while [ "$ATTEMPT" -lt 100 ]; do
    if ! kill -0 "$UVICORN_PID" 2>/dev/null; then
        echo "Uvicorn exited before becoming ready." >&2
        wait "$UVICORN_PID" || true
        exit 1
    fi

    if curl --silent --show-error --fail http://127.0.0.1:8000/health >/dev/null 2>&1; then
        READY=1
        break
    fi

    ATTEMPT=$((ATTEMPT + 1))
    sleep 0.1
done

if [ "$READY" -ne 1 ]; then
    echo "FastAPI did not become ready within 10 seconds." >&2
    exit 1
fi

echo "FastAPI is ready. Starting YAML-configured Java baseline client."
java -jar /app/java-client.jar
