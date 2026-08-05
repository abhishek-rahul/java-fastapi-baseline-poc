import asyncio
import threading
from datetime import datetime, timezone
from time import perf_counter_ns

from fastapi import FastAPI

from app.models import ProcessRequest, ProcessResponse

app = FastAPI(
    title="Java-FastAPI REST Baseline",
    version="1.0.0",
)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@app.get("/health")
async def health() -> dict[str, str]:
    return {
        "status": "UP",
        "service": "python-fastapi-baseline",
        "pythonMode": "asyncio",
    }


@app.post("/api/v1/process", response_model=ProcessResponse)
async def process(request: ProcessRequest) -> ProcessResponse:
    start_time = utc_now_iso()
    start_ns = perf_counter_ns()

    # This represents non-blocking async work in the current FastAPI service.
    await asyncio.sleep(request.delay_ms / 1000.0)

    end_ns = perf_counter_ns()
    end_time = utc_now_iso()

    return ProcessResponse(
        request_id=request.request_id,
        original_message=request.message,
        processed_message=request.message.upper(),
        delay_ms=request.delay_ms,
        python_start_time=start_time,
        python_end_time=end_time,
        python_execution_time_ms=(end_ns - start_ns) / 1_000_000,
        event_loop_thread=threading.current_thread().name,
    )
