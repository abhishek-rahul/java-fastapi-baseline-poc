# Phase Status

Phase: 1 — Dual-Mode Compatibility Foundation
State: COMPLETE
Date: 2026-08-05
Environment: Windows 10 host; Java 21.0.9; Maven 3.9.10; Python 3.13.5 host; Docker 28.0.4 with Linux containers; image uses Java 17 and Python 3.12

## Repository findings

- The original Java path was `BaselineApplication -> RestProcessingClient -> HttpUtil/OkHttp3`.
- The original no-argument JAR command ignored arguments and used HTTP configuration from `application.yml`.
- The FastAPI application has `/health` and async `/api/v1/process`; it has no custom middleware, dependencies, or lifespan hooks.
- The managed runtime is supported and exercised in the Linux container because the Windows host environment is not the documented UDS target.

## Implemented

- Added backward-compatible `HTTP`/`MANAGED_RUNTIME` CLI selection; no argument still selects HTTP.
- Added the global `PythonCallUtil` lifecycle with mode-specific validation, idempotent equal initialization, conflicting-initialization rejection, clear non-ready failures, and idempotent shutdown.
- Moved OkHttp ownership behind `HttpPythonCallExecutor`; `RestProcessingClient` now uses the common request/response boundary.
- Added one long-lived Java-owned CPython worker, bounded FIFO admission, one active Python request, UUID correlation, bounded framing, deadlines, and deterministic process/socket cleanup.
- Added a version-1 UDS protocol with ready, request, response, error, shutdown, and shutdown acknowledgement messages.
- Added `python_runtime` outside `app`; it imports `app.main:app`, runs ASGI lifespan, and invokes HTTP requests through the ASGI application boundary.
- Added a separate real-boundary `Phase1E2ERunner`; no diagnostic branch was added to `BaselineApplication`.
- Updated the container entrypoint so HTTP starts Uvicorn and managed mode does not.
- Kept existing tests and adapted their setup to the new global utility boundary; no tests were added.

## Files changed

- `java-client/src/main/java/com/example/baseline/BaselineApplication.java`
- `java-client/src/main/java/com/example/baseline/client/RestProcessingClient.java`
- `java-client/src/main/java/com/example/baseline/utils/config/ApplicationConfig.java`
- `java-client/src/main/resources/application.yml`
- `java-client/src/main/java/com/example/baseline/utils/python/` — common boundary, HTTP executor, lifecycle, protocol, and managed runtime
- `java-client/src/main/java/com/example/baseline/e2e/Phase1E2ERunner.java`
- `python-fastapi/python_runtime/` — worker bootstrap and ASGI adapter
- `Dockerfile`
- `docker-entrypoint.sh`
- Existing Java test setup files for compatibility with selected-mode validation/global lifecycle
- `docs/codex/PHASE_STATUS.md`

## Commands executed

- `cd python-fastapi; python -m uvicorn app.main:app --host 127.0.0.1 --port 8000`
  Result: FAIL on the Windows host — its Python 3.13 environment did not have Uvicorn installed; real HTTP and managed E2E were therefore run in the repository's Python 3.12 container.
- Initial `docker run --rm java-fastapi-runtime-poc MANAGED_RUNTIME`
  Result: FAIL — timed out after readiness because `Channels` stream wrappers serialized blocking reads and writes on the UDS `SocketChannel`; replaced them with full-duplex bounded `ByteBuffer` channel operations, then reran successfully.
- `cd java-client; mvn -DskipTests clean package`
  Result: PASS — 17 main sources and 3 test sources compiled for Java 17; shaded JAR built successfully.
- `cd java-client; mvn test`
  Result: PASS — 8 existing tests, 0 failures, 0 errors, 0 skipped.
- `docker build --no-cache -t java-fastapi-runtime-poc .`
  Result: PASS — clean multi-stage Java 17/Python 3.12 image build.
- `docker build -t java-fastapi-runtime-poc .`
  Result: PASS — rebuilt final image after the last lifecycle and selected-mode validation corrections.
- `docker run --rm java-fastapi-runtime-poc HTTP`
  Result: PASS — Uvicorn started with 4 workers; 1,000/1,000 requests succeeded; batch 3,130.941 ms; average Python work 10.188 ms.
- `docker run --rm java-fastapi-runtime-poc MANAGED_RUNTIME`
  Result: PASS — Uvicorn was not started; one worker PID 26 handled 1,000/1,000 requests; batch 11,623.956 ms; socket removal reported true.
- `docker run --rm java-fastapi-runtime-poc HTTP E2E`
  Result: PASS — real OkHttp/Uvicorn path verified success response, headers, 422 validation, non-success mapping, lifecycle rules, and 4 concurrent callers.
- `docker run --rm java-fastapi-runtime-poc MANAGED_RUNTIME E2E`
  Result: PASS — real CPython/UDS/ASGI path verified the same scenarios; one PID was used for the entire run and socket removal reported true.
- `1..3 | ForEach-Object { docker run --rm java-fastapi-runtime-poc MANAGED_RUNTIME E2E }`
  Result: PASS — three runs used one worker each (PIDs 27, 27, and 26 in their separate containers); all reported `socketRemoved=true`.
- `docker run --rm --entrypoint /bin/sh java-fastapi-runtime-poc -c '<start Uvicorn; java -jar /app/java-client.jar; stop Uvicorn>'`
  Result: PASS — preserved no-argument command selected HTTP and completed 1,000/1,000 requests in 3,148.540 ms.
- `docker ps -a --filter ancestor=java-fastapi-runtime-poc --format '{{.ID}} {{.Status}} {{.Names}}'`
  Result: PASS — no remaining Phase 1 containers after shutdown checks.
- `git hash-object python-fastapi/app/main.py` and comparison with `git rev-parse HEAD:python-fastapi/app/main.py`
  Result: PASS — both `a1b87ce476411b7f6f3cf453c8ad8644f0787c65`.
- `git hash-object python-fastapi/app/models.py` and comparison with `git rev-parse HEAD:python-fastapi/app/models.py`
  Result: PASS — both `199858dab3feb81d6749e8dcfceefba31bf94f6f`.

## E2E gates

- PASS — Java packages successfully from source.
- PASS — HTTP starts real Uvicorn and completes the configured workload through OkHttp3.
- PASS — managed mode starts one real CPython worker itself, does not start Uvicorn, uses a real UDS, invokes ASGI, and completes the same workload.
- PASS — valid E2E requests in both modes returned status 200, Content-Type, and the expected business response fields.
- PASS — invalid input in both real paths returned FastAPI status 422 and validation details; `RestProcessingClient` preserved non-success status/body context.
- PASS — sequential and concurrent managed requests reused the one PID printed at readiness and shutdown.
- PASS — 4 concurrent Java callers safely shared the global runtime; lifecycle locking does not cover calls or waits.
- PASS — equal initialization was idempotent, conflicting initialization was rejected, and post-close calls were rejected.
- PASS — three repeated start/stop runs reported socket removal and left no Phase 1 container or worker process.
- PASS — switching back to HTTP required only the mode argument.
- PASS — `python-fastapi/app/main.py` and `python-fastapi/app/models.py` are unchanged.

## Known limitations

- Phase 1 intentionally has one worker and one active ASGI request; concurrent Java requests wait in a bounded queue.
- There is no worker pool, worker restart/replacement, health loop, multiple in-flight task support, or automatic request retry.
- Managed UDS E2E was executed in the Linux container, not on the Windows host.
- The existing FastAPI/Pydantic combination emits alias-related warnings during startup, but both modes produced the expected request/response behavior; application files were not changed.

## Deferred work

- Phase 2 multi-process worker pool, replacement, health, and scheduling.
- Phase 3 multiple correlated in-flight ASGI tasks per worker.
- Phase 4 observability, deployment hardening, and operator rollout work.
- New protocol/lifecycle unit and component tests remain deferred until explicitly requested after all four phases.

## Final statement

Phase 1 complete
