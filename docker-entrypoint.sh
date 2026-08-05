#!/bin/sh
set -eu

MODE="${1:-HTTP}"
RUN_TARGET="${2:-APPLICATION}"
UVICORN_WORKERS="${UVICORN_WORKERS:-4}"

PYTHON_PROJECT_DIR="/app/python-fastapi"
UVICORN_PID=""

cleanup() {
    if [ -n "$UVICORN_PID" ] && kill -0 "$UVICORN_PID" 2>/dev/null; then
        kill -TERM "$UVICORN_PID" 2>/dev/null || true
        wait "$UVICORN_PID" 2>/dev/null || true
    fi
}

trap cleanup EXIT INT TERM

case "$MODE" in
    HTTP)
        echo "Starting baseline TCP server: host=127.0.0.1, port=8000, uvicornWorkers=$UVICORN_WORKERS"
        cd "$PYTHON_PROJECT_DIR"
        python3 -m uvicorn app.main:app \
            --host 127.0.0.1 \
            --port 8000 \
            --workers "$UVICORN_WORKERS" \
            --no-access-log &
        UVICORN_PID=$!
        ;;
    MANAGED_RUNTIME)
        echo "Starting Java with the configured Managed Python Runtime worker pool; Uvicorn is not started."
        ;;
    *)
        echo "Unsupported mode '$MODE'. Expected HTTP or MANAGED_RUNTIME." >&2
        exit 2
        ;;
esac

if [ "$MODE" = "HTTP" ]; then
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
    echo "FastAPI is ready. Starting YAML-configured Java client."
fi

cd /app/java-client
case "$RUN_TARGET" in
    APPLICATION)
        java -jar /app/java-client.jar "$MODE"
        ;;
    E2E)
        java -cp /app/java-client.jar com.example.baseline.e2e.Phase1E2ERunner "$MODE"
        ;;
    E2E_SHUTDOWN_DRAIN)
        java -cp /app/java-client.jar com.example.baseline.e2e.Phase1E2ERunner "$MODE" SHUTDOWN_DRAIN
        ;;
    E2E_SHUTDOWN_TIMEOUT)
        java -cp /app/java-client.jar com.example.baseline.e2e.Phase1E2ERunner "$MODE" SHUTDOWN_TIMEOUT
        ;;
    PHASE2_E2E)
        SCENARIO="${3:-}"
        if [ "$MODE" != "MANAGED_RUNTIME" ] || [ -z "$SCENARIO" ]; then
            echo "PHASE2_E2E requires: MANAGED_RUNTIME PHASE2_E2E <SCENARIO>." >&2
            exit 2
        fi
        java -cp /app/java-client.jar com.example.baseline.e2e.Phase2E2ERunner "$SCENARIO"
        ;;
    PHASE3_E2E)
        SCENARIO="${3:-}"
        if [ "$MODE" != "MANAGED_RUNTIME" ] || [ -z "$SCENARIO" ]; then
            echo "PHASE3_E2E requires: MANAGED_RUNTIME PHASE3_E2E <SCENARIO>." >&2
            exit 2
        fi
        java -Dorg.slf4j.simpleLogger.defaultLogLevel=debug \
            -cp /app/java-client.jar com.example.baseline.e2e.Phase3E2ERunner "$SCENARIO"
        ;;
    *)
        echo "Unsupported run target '$RUN_TARGET'. Expected APPLICATION, E2E, E2E_SHUTDOWN_DRAIN, E2E_SHUTDOWN_TIMEOUT, PHASE2_E2E, or PHASE3_E2E." >&2
        exit 2
        ;;
esac
