# Phase Status

Phase: 2 — Reliable Multi-Process Worker Pool
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
- Corrected the Phase 1 shutdown race: queued calls fail immediately, one transmitted active call gets the configured drain window, timeout failure is deterministic, and dispatcher ownership cannot abandon a dequeued future.

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

## Shutdown race fix evidence

- `cd java-client; mvn -DskipTests clean package`
  Result: PASS - 17 main sources and 3 existing test sources compiled; the shaded JAR was built.
- `cd java-client; mvn test`
  Result: PASS - 8 existing tests, 0 failures, 0 errors, 0 skipped.
- `docker build -t java-fastapi-runtime-poc .`
  Result: PASS - the image containing the focused shutdown fix and E2E scenarios built successfully.
- `docker run --rm java-fastapi-runtime-poc MANAGED_RUNTIME E2E_SHUTDOWN_DRAIN`
  Result: PASS - the transmitted 1,000 ms request completed normally, the queued request failed, new calls were rejected, all futures completed, shutdown took 742.800 ms, and `socketRemoved=true` was reported for worker PID 27.
- `docker run --rm java-fastapi-runtime-poc MANAGED_RUNTIME E2E_SHUTDOWN_TIMEOUT`
  Result: PASS - the transmitted 10,000 ms request exceeded the 5,000 ms shutdown window, its caller received the deterministic shutdown-timeout failure, its future was done, shutdown returned in 5,003.719 ms, and `socketRemoved=true` was reported for worker PID 27.
- Post-fix `docker run --rm java-fastapi-runtime-poc HTTP E2E` and `docker run --rm java-fastapi-runtime-poc MANAGED_RUNTIME E2E`
  Result: PASS - both complete standard E2E harnesses passed without regression.
- Post-fix `docker run --rm java-fastapi-runtime-poc HTTP` and `docker run --rm java-fastapi-runtime-poc MANAGED_RUNTIME`
  Result: PASS - both modes completed 1,000/1,000 requests; HTTP batch 3,257.634 ms and managed batch 11,798.846 ms; managed shutdown reported `socketRemoved=true`.
- Post-fix `docker ps -a --filter ancestor=java-fastapi-runtime-poc --format '{{.ID}} {{.Status}} {{.Names}}'`
  Result: PASS - no containers or managed workers remained.
- PASS - shutdown drain allows the transmitted active request to complete while rejecting new calls and failing queued calls, with every caller future terminal.
- PASS - an active request beyond the shutdown deadline fails deterministically, shutdown remains bounded, and worker/UDS cleanup completes.

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

---

# Phase 2 — Reliable Multi-Process Worker Pool

State: COMPLETE
Date: 2026-08-05
Environment: Windows 10 host with Docker Linux containers; final image uses Java 17 and Python 3.12

## Implemented

- Converted `ManagedPythonRuntime` into one application-wide fixed worker-pool owner with a bounded FIFO admission queue and an assignment-only dispatcher.
- Added `ManagedPythonWorker`, with one CPython process generation, UDS connection, one active assignment, request runner, response reader, state transitions, failure handling, and cleanup per worker.
- Added stable worker slots, generation-specific socket paths, degraded/unavailable pool behavior, capped exponential restart backoff, and rolling restart budgets.
- Added distinct queue-wait and request-execution deadlines. A timed-out or ambiguously failed request is never retried and its worker is poisoned and replaced.
- Added concurrent pool shutdown using one common absolute deadline: queued/dispatcher-owned requests fail, active workers drain together, over-time requests fail deterministically, and surviving processes are forcibly terminated.
- Added Phase 2 configuration for worker count, queue timeout, and restart policy. HTTP mode continues to ignore managed-only configuration.
- Added the real-process `Phase2E2ERunner` and `MANAGED_RUNTIME PHASE2_E2E <SCENARIO>` container routing. No unit tests or production failure-injection hooks were added.

## Files changed

- `java-client/src/main/java/com/example/baseline/utils/python/ManagedPythonRuntime.java`
- `java-client/src/main/java/com/example/baseline/utils/python/ManagedPythonWorker.java`
- `java-client/src/main/java/com/example/baseline/utils/config/ApplicationConfig.java`
- `java-client/src/main/resources/application.yml`
- `java-client/src/main/java/com/example/baseline/e2e/Phase2E2ERunner.java`
- `docker-entrypoint.sh`
- `docs/codex/PHASE_STATUS.md`

## Commands and observed results

- `cd java-client; mvn -DskipTests clean package`
  Result: PASS — 19 main sources and 3 existing test sources compiled for Java 17; the shaded JAR was built.
- `cd java-client; mvn test`
  Result: PASS — 8 existing tests, 0 failures, 0 errors, 0 skipped.
- `docker build --no-cache --progress=plain -t java-fastapi-runtime-poc .`
  Result: PASS — the exact final source built in a clean Java 17/Python 3.12 image in 88.3 seconds.
- `docker run --rm java-fastapi-runtime-poc HTTP`
  Result: PASS — real Uvicorn/OkHttp completed 1,000/1,000 requests; batch 3,033.310 ms; average Python work 10.195 ms.
- `docker run --rm java-fastapi-runtime-poc HTTP E2E`
  Result: PASS — the existing real HTTP success, headers, 422/non-2xx, lifecycle, and concurrent-caller regression passed, including a post-no-cache-image run.
- `docker run --rm java-fastapi-runtime-poc MANAGED_RUNTIME`
  Result: PASS — four Java-owned CPython workers (PIDs 31, 32, 33, and 34) completed 1,000/1,000 requests without Uvicorn; batch 3,373.816 ms; every socket and the pool directory were removed.
- `docker run --rm java-fastapi-runtime-poc MANAGED_RUNTIME E2E`
  Result: PASS — the existing managed success, headers, 422/non-2xx, lifecycle, and concurrent-caller regression passed with four workers and clean shutdown.
- `docker run --rm java-fastapi-runtime-poc MANAGED_RUNTIME PHASE2_E2E PARALLEL`
  Result: PASS — final no-cache-image run used PIDs 30, 31, 32, and 33; all four overlapped; eight 500 ms requests completed in 1,044.617 ms; the same PID set was reused; all sockets and the pool directory were removed.
- `docker run --rm java-fastapi-runtime-poc MANAGED_RUNTIME PHASE2_E2E QUEUE`
  Result: PASS — with one worker, queue capacity 2, and six calls, five timed out under the queue policy; all futures were terminal; elapsed time was 1,049 ms.
- `docker run --rm java-fastapi-runtime-poc MANAGED_RUNTIME PHASE2_E2E IDLE_FAILURE`
  Result: PASS — idle PID 31 was killed, replacement PID 59 became READY, restored four-way parallel capacity, and shutdown removed all four worker sockets/processes.
- `docker run --rm java-fastapi-runtime-poc MANAGED_RUNTIME PHASE2_E2E BUSY_FAILURE`
  Result: PASS — busy PID 34 was killed; exactly one active request failed without retry, the other three succeeded, replacement PID 63 became ready, and the pool remained usable.
- `docker run --rm java-fastapi-runtime-poc MANAGED_RUNTIME PHASE2_E2E STARTUP_FAILURE`
  Result: PASS — two-worker initialization with a nonexistent Python command failed in 54 ms and left no child PID or runtime directory.
- `docker run --rm java-fastapi-runtime-poc MANAGED_RUNTIME PHASE2_E2E RESTART_EXHAUSTION`
  Result: PASS — one slot used initial PID 28 and READY replacement PIDs 34 and 43, honored 50/100 ms backoffs, stopped after two restart attempts, and rejected an unavailable call in 0 ms.
- `docker run --rm java-fastapi-runtime-poc MANAGED_RUNTIME PHASE2_E2E REQUEST_TIMEOUT`
  Result: PASS — timed-out PID 28 was poisoned without retry, replacement PID 41 became READY, and a subsequent request succeeded.
- `docker run --rm java-fastapi-runtime-poc MANAGED_RUNTIME PHASE2_E2E SHUTDOWN`
  Result: PASS — four active requests drained concurrently, four queued requests failed, new calls were rejected, all futures were terminal, and shutdown/process/socket cleanup completed in 694 ms.
- `docker run --rm java-fastapi-runtime-poc MANAGED_RUNTIME PHASE2_E2E SHUTDOWN_TIMEOUT`
  Result: PASS — four over-time active requests failed deterministically; the shared 500 ms deadline produced a 517 ms shutdown; all PIDs, sockets, and caller futures were cleaned.
- `1..3 | ForEach-Object { docker run --rm java-fastapi-runtime-poc MANAGED_RUNTIME PHASE2_E2E PARALLEL }`
  Result: PASS — all three runs had four-way overlap, stable per-run PID reuse, request wall times of 1,047.595/1,067.448/1,059.742 ms, and `directoryRemoved=true`.
- Final exact-image matrix: `PARALLEL`, `QUEUE`, `IDLE_FAILURE`, `BUSY_FAILURE`, `STARTUP_FAILURE`, `RESTART_EXHAUSTION`, `REQUEST_TIMEOUT`, `SHUTDOWN`, and `SHUTDOWN_TIMEOUT`
  Result: PASS — all nine scenarios passed against the final no-cache image. Latest observations included 1,052.967 ms parallel wall time with four-way overlap, five bounded queue failures with every future terminal, PID 34 replaced by PID 59 after idle death, one non-retried busy failure with PID 63 replacement, 49 ms bounded startup failure, restart exhaustion after PIDs 28/34/43, request-timeout replacement PID 28→41, 694 ms drain shutdown, and 510 ms forced shutdown against the shared 500 ms deadline. Every reported pool cleanup had `directoryRemoved=true`.
- `docker ps -a --filter ancestor=java-fastapi-runtime-poc`
  Result: PASS — no Phase 2 containers remained after the final and repeated cleanup checks.
- Hash comparisons against `HEAD` for `python-fastapi/app/main.py`, `models.py`, `python_runtime/worker_runtime.py`, and `asgi_adapter.py`
  Result: PASS — working-tree and `HEAD` hashes matched for all four files.

## Acceptance gates

- PASS — Java clean package and all existing tests succeed; no new unit tests were added.
- PASS — the unchanged OkHttp/Uvicorn path and mode-only HTTP rollback work.
- PASS — managed mode creates four distinct Java-owned CPython workers and uses UDS/ASGI without Uvicorn or internal HTTP.
- PASS — sequential calls reuse the pool; four delayed requests overlap in four processes; each worker accepts only one active assignment.
- PASS — bounded FIFO admission, queue capacity, queue wait, frame size, startup, request, restart, and shutdown policies are enforced.
- PASS — idle and busy worker death are detected; the busy request fails without retry; healthy workers continue; replacement generations use new PIDs and unique sockets.
- PASS — request timeout poisons and replaces its worker; total startup failure is bounded and clean.
- PASS — degraded and unavailable transitions were observed; capped backoff and the rolling attempt budget stop restart loops.
- PASS — shutdown drains all workers concurrently against one deadline, fails queued work, rejects new work, and deterministically completes over-time caller futures.
- PASS — repeated runs leave no CPython descendants, UDS files, runtime directories, or containers.
- PASS — the FastAPI application, Python runtime adapter, HTTP utility/executor, client, main application, and public `PythonCallUtil` API remain unchanged.

## Phase 2 limitations

- The pool is fixed-size. Each worker runs one sequential ASGI request; multiple in-flight ASGI tasks per worker remain Phase 3 work.
- No ping/pong health loop, autoscaling, TCP transport, advanced metrics, rollout controls, route classification, CPU/I/O split, GIL modification, or automatic business-request retry was added.
- Managed UDS E2E was executed in Linux containers, not on the Windows host.
- The existing FastAPI/Pydantic dependency combination emits alias-related startup warnings; request/response behavior passed in both modes and application files were not changed.

## Final statement

Phase 2 complete
