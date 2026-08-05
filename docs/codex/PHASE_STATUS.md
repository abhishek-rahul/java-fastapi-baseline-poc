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

---

## Focused Phase 2 unhealthy-worker termination fix

Date: 2026-08-05

- Root cause corrected: an unhealthy worker received `Process.destroy()`, but its slot supervisor then used an unbounded `Process.waitFor()`. A stopped or otherwise non-responsive CPython process could therefore prevent cleanup and replacement indefinitely.
- `ManagedPythonWorker` now gives an unhealthy process a bounded graceful exit opportunity, escalates to `destroyForcibly()` when it remains alive, waits only within the remaining `shutdown-timeout-ms` budget, removes the generation socket, and releases the slot supervisor for replacement.
- Added real-process scenario `UNHEALTHY_FORCE_TERMINATION`: one worker is suspended with `SIGSTOP`, its request times out, the graceful stop cannot complete, forced termination is observed, and a new generation handles a subsequent request.

### Commands and observed results

- `cd java-client; mvn -DskipTests clean package`
  Result: PASS — 19 main sources and 3 existing test sources compiled for Java 17; shaded JAR built; total time 7.969 seconds.
- `cd java-client; mvn test`
  Result: PASS — 8 existing tests, 0 failures, 0 errors, 0 skipped.
- `docker build --no-cache --progress=plain -t java-fastapi-runtime-poc .`
  Result: PASS — exact corrected source built into the Java 17/Python 3.12 image in 95.1 seconds.
- Initial `UNHEALTHY_FORCE_TERMINATION` run
  Result: E2E harness portability failure — `/bin/kill` was absent in the Debian image. The harness was corrected to send the same real `SIGSTOP` through `/bin/sh`; production runtime code was not changed for this issue.
- `docker run --rm java-fastapi-runtime-poc MANAGED_RUNTIME PHASE2_E2E UNHEALTHY_FORCE_TERMINATION`
  Result: PASS — suspended original PID 27 timed out, forced fallback was observed, termination completed in 877 ms against `shutdown-timeout-ms=1000`, replacement PID 43 served a successful request, the caller future was terminal, and `worker-1-g1.sock` was absent.
- `docker run --rm java-fastapi-runtime-poc HTTP E2E`
  Result: PASS — existing real OkHttp/Uvicorn E2E passed with four concurrent callers.
- `docker run --rm java-fastapi-runtime-poc MANAGED_RUNTIME E2E`
  Result: PASS — existing real managed-runtime E2E passed with four concurrent callers.
- `docker run --rm java-fastapi-runtime-poc MANAGED_RUNTIME PHASE2_E2E IDLE_FAILURE`
  Result: PASS — killed idle PID 34 was removed, replacement PID 60 restored four-worker capacity, and later work succeeded.
- `docker run --rm java-fastapi-runtime-poc MANAGED_RUNTIME PHASE2_E2E BUSY_FAILURE`
  Result: PASS — killed busy PID 34 caused exactly one non-retried active failure, three other active calls succeeded, and replacement PID 63 restored the pool.
- `docker run --rm java-fastapi-runtime-poc MANAGED_RUNTIME PHASE2_E2E REQUEST_TIMEOUT`
  Result: PASS — timed-out PID 28 was poisoned without retry, replacement PID 41 became ready, and a subsequent call succeeded.
- `docker run --rm java-fastapi-runtime-poc MANAGED_RUNTIME PHASE2_E2E RESTART_EXHAUSTION`
  Result: PASS — generations used PIDs 28, 34, and 43; restart stopped after two attempts; unavailable work failed in 0 ms with no infinite restart.
- `docker run --rm java-fastapi-runtime-poc MANAGED_RUNTIME PHASE2_E2E SHUTDOWN`
  Result: PASS — four active requests drained, four queued requests failed, new calls were rejected, every future was terminal, and PIDs 31, 32, 33, and 34 were removed in a 695 ms shutdown.
- `docker run --rm java-fastapi-runtime-poc MANAGED_RUNTIME PHASE2_E2E SHUTDOWN_TIMEOUT`
  Result: PASS — four over-time active requests failed deterministically, every future was terminal, and PIDs 33, 34, 35, and 36 were removed in 519 ms against one 500 ms common deadline.
- `docker ps -a --filter ancestor=java-fastapi-runtime-poc --format "{{.ID}} {{.Status}} {{.Names}}"`
  Result: PASS — output was empty; no E2E container remained.
- `git diff --name-only -- python-fastapi/app python-fastapi/python_runtime java-client/src/main/java/com/example/baseline/utils/http java-client/src/main/java/com/example/baseline/client/RestProcessingClient.java java-client/src/main/java/com/example/baseline/BaselineApplication.java`
  Result: PASS — output was empty; FastAPI application, Python runtime adapter, HTTP utility path, REST client, and application entry point were unchanged by this fix.

### Focused acceptance result

PASS — unhealthy-process termination is bounded, the active request completes exceptionally exactly once, slot replacement proceeds with a new PID, no caller future/process/socket/container is leaked, and all requested Phase 2 regressions pass. No Phase 3 work was started.

---

# Phase 3 - Unified Async ASGI Concurrency

State: COMPLETE
Date: 2026-08-05
Environment: Windows 10 host with Docker Linux containers; final image uses Java 17 and Python 3.12

## Implemented

- Added bounded `max-in-flight-per-worker` configuration and readiness verification, with Phase 2 scenarios pinned to capacity one.
- Replaced each worker's single active assignment with a bounded request-ID registry, in-flight counter, bounded outbound queue, one non-waiting socket writer, and one correlated response reader.
- Added capacity-aware, one-token worker publication. The FIFO dispatcher reserves one slot, republishes remaining capacity at the queue tail, and never waits for Python execution or a response.
- Updated the Python worker to read requests continuously, run each request as an independent bounded asyncio task through `app.main:app`, serialize complete response frames, and permit correlated out-of-order completion.
- Added exact terminal ownership for response, request error, pre-transmission timeout, transmitted timeout, worker failure, and shutdown paths. Ambiguous failures poison the worker and never retry business work.
- Extended common-deadline shutdown to drain multiple active assignments concurrently and force all remaining workers/futures terminal at the deadline.
- Added the real-process `Phase3E2ERunner` and `MANAGED_RUNTIME PHASE3_E2E <SCENARIO>` routing. No unit tests or production failure-injection hooks were added.

## Files changed

- `java-client/src/main/java/com/example/baseline/utils/python/ManagedPythonRuntime.java`
- `java-client/src/main/java/com/example/baseline/utils/python/ManagedPythonWorker.java`
- `java-client/src/main/java/com/example/baseline/utils/config/ApplicationConfig.java`
- `java-client/src/main/resources/application.yml`
- `python-fastapi/python_runtime/worker_runtime.py`
- `java-client/src/main/java/com/example/baseline/e2e/Phase2E2ERunner.java`
- `java-client/src/main/java/com/example/baseline/e2e/Phase3E2ERunner.java`
- `docker-entrypoint.sh`
- `docs/codex/PHASE_STATUS.md`

## Commands and observed results

- `cd java-client; mvn -DskipTests clean package`
  Result: PASS - 20 main sources and 3 existing test sources compiled for Java 17; the shaded JAR was built in 8.156 seconds.
- `cd java-client; mvn test`
  Result: PASS - 8 existing tests, 0 failures, 0 errors, 0 skipped; total time 8.497 seconds.
- Python source compilation of `python-fastapi/python_runtime/worker_runtime.py`
  Result: PASS - the modified worker runtime compiled successfully.
- `docker build --no-cache --progress=plain -t java-fastapi-runtime-poc .`
  Result: PASS - the exact final source built in a clean Java 17/Python 3.12 image in 81.8 seconds.
- `docker run --rm java-fastapi-runtime-poc HTTP`
  Result: PASS - real Uvicorn/OkHttp completed 1,000/1,000 requests; batch 3,072.052 ms; average Python work 10.160 ms.
- `docker run --rm java-fastapi-runtime-poc HTTP E2E`
  Result: PASS - the existing real HTTP success, headers, 422/non-2xx, lifecycle, and concurrent-caller regression passed.
- `docker run --rm java-fastapi-runtime-poc MANAGED_RUNTIME`
  Result: PASS - four Java-owned CPython workers completed 1,000/1,000 requests without Uvicorn; batch 3,022.045 ms; average Python work 10.197 ms.
- `docker run --rm java-fastapi-runtime-poc MANAGED_RUNTIME E2E`
  Result: PASS - the existing managed success, headers, 422/non-2xx, lifecycle, and concurrent-caller regression passed.
- Final Phase 2 capacity-one matrix: `PARALLEL`, `QUEUE`, `IDLE_FAILURE`, `BUSY_FAILURE`, `STARTUP_FAILURE`, `RESTART_EXHAUSTION`, `REQUEST_TIMEOUT`, `UNHEALTHY_FORCE_TERMINATION`, `SHUTDOWN`, and `SHUTDOWN_TIMEOUT`
  Result: PASS - all ten scenarios passed against the final no-cache image. Observations included four-way process overlap in 1,035.722 ms, bounded queue failure with every future terminal, idle PID 34 replaced by PID 59, one non-retried busy failure with replacement PID 63, bounded startup failure in 100 ms, restart exhaustion after three generations, request-timeout replacement PID 29 to PID 42, forced unhealthy termination in 891 ms, four-request graceful drain in 701 ms, and a 515 ms forced shutdown against one 500 ms deadline.
- `docker run --rm java-fastapi-runtime-poc MANAGED_RUNTIME PHASE3_E2E SAME_WORKER`
  Result: PASS - one PID executed four overlapping 500 ms ASGI requests with max-in-flight four; wall time 539.601 ms; every response mapping was correct.
- `docker run --rm java-fastapi-runtime-poc MANAGED_RUNTIME PHASE3_E2E MULTI_WORKER`
  Result: PASS - two PIDs each accepted at most three requests, total overlap reached six, wall time was 545.175 ms, and response mappings were correct.
- `docker run --rm java-fastapi-runtime-poc MANAGED_RUNTIME PHASE3_E2E OUT_OF_ORDER`
  Result: PASS - completion order `[order-1, order-3, order-2, order-0]` differed from submission order `[order-0, order-1, order-2, order-3]`; all four IDs and bodies correlated correctly.
- `docker run --rm java-fastapi-runtime-poc MANAGED_RUNTIME PHASE3_E2E CAPACITY`
  Result: PASS - one worker with capacity two and queue capacity two accepted no more than two overlapping requests; four of six calls failed within the bounded policy; every future was terminal; elapsed time was 1,042 ms.
- `docker run --rm java-fastapi-runtime-poc MANAGED_RUNTIME PHASE3_E2E MULTI_ACTIVE_FAILURE`
  Result: PASS - killing real PID 29 failed its three assignments without retry, all three assignments on the other worker succeeded, replacement PID 52 restored capacity, and every future was terminal.
- `docker run --rm java-fastapi-runtime-poc MANAGED_RUNTIME PHASE3_E2E MULTI_ACTIVE_TIMEOUT`
  Result: PASS - a transmitted timeout poisoned PID 28, all three sibling assignments failed without retry, replacement PID 44 served subsequent work, and every future was terminal.
- `docker run --rm java-fastapi-runtime-poc MANAGED_RUNTIME PHASE3_E2E CORRELATED_ERROR`
  Result: PASS - one genuine request-specific ASGI adapter error failed only that request; three siblings succeeded; PID 28 was reused and released capacity served a subsequent request.
- `docker run --rm java-fastapi-runtime-poc MANAGED_RUNTIME PHASE3_E2E SHUTDOWN`
  Result: PASS - six active tasks drained concurrently, four queued calls failed, new calls were rejected, every future was terminal, shutdown took 346 ms, and PIDs 29 and 30 plus their runtime directory were removed.
- `docker run --rm java-fastapi-runtime-poc MANAGED_RUNTIME PHASE3_E2E SHUTDOWN_TIMEOUT`
  Result: PASS - six active tasks exceeded one shared 500 ms deadline, all failed deterministically, shutdown returned in 506 ms, and PIDs 29 and 30 plus their runtime directory were removed.
- Three additional `SAME_WORKER` and `MULTI_WORKER` repetitions
  Result: PASS - same-worker overlap remained four with 531.332-544.501 ms wall times; multi-worker overlap remained six with 530.586-534.362 ms wall times; every run removed all child processes, worker sockets, and pool directories.
- `docker ps -a --filter ancestor=java-fastapi-runtime-poc --format "{{.ID}} {{.Status}} {{.Names}}"`
  Result: PASS - output was empty after the final and repeated runs; no test container remained.
- `git diff --check`
  Result: PASS - no whitespace errors were reported.
- Source audit using `git diff --name-only` and `rg`
  Result: PASS - FastAPI application files, `asgi_adapter.py`, HTTP utility/executor, REST client, CLI entry point, public `PythonCallUtil`, and protocol framing remained unchanged; no CPU/I/O split, route classification, separate pool, or GIL manipulation was introduced.

## Acceptance gates

- PASS - Java clean package and all eight existing tests succeed; no new unit tests were added.
- PASS - unchanged HTTP/Uvicorn/OkHttp application and E2E flows pass; rollback remains the `HTTP` mode argument only.
- PASS - managed execution still uses Java-owned CPython processes, UDS, one shared ASGI lifespan per process, and direct `app.main:app` invocation without Uvicorn or internal HTTP.
- PASS - one process executes four overlapping ASGI tasks; two processes execute six overlapping tasks while respecting three slots per worker.
- PASS - correlated responses may finish out of order without caller or payload mismatch.
- PASS - worker availability is capacity-aware and bounded; FIFO admission, queue timeout, frame size, startup, request, restart, and shutdown bounds remain enforced.
- PASS - request-specific Python errors release only their assignment; transmitted timeout and worker death fail all affected assignments once, poison the generation, and restore capacity under a new PID without retry.
- PASS - Phase 2 capacity-one behavior, degraded/unavailable handling, restart budgets, and bounded unhealthy-process termination remain intact.
- PASS - graceful shutdown drains all active tasks concurrently; forced shutdown uses one pool deadline and leaves every future terminal.
- PASS - repeated runs leave no live CPython descendant, UDS socket, runtime directory, or container; process exit also closes each generation's descriptors and asyncio tasks.
- PASS - FastAPI application code and the ASGI adapter remain unchanged; source contains no CPU/I/O split or GIL manipulation.

## Phase 3 limitations

- Concurrency is fixed by configured process count and per-worker capacity; there is no autoscaling or per-request cancellation protocol.
- A timeout after transmission poisons the complete worker generation because Python execution is ambiguous; requests are never retried automatically.
- Responses are buffered framed messages; TCP transport and streaming remain deferred.
- Managed UDS E2E was executed in Linux containers, not on the Windows host.
- The existing FastAPI/Pydantic dependency combination emits alias-related warnings; request/response behavior passed and FastAPI application files were not changed.
- Advanced metrics, production observability, rollout controls, and new unit/component tests remain deferred.

## Final statement

Phase 3 complete

---

## Focused Phase 3 CAPACITY_EXCEEDED handling correction

Date: 2026-08-06

- Root cause corrected: Java treated every correlated Python `error` frame as request-specific, so `CAPACITY_EXCEEDED` released one slot and could republish a worker whose Java/Python capacity accounting disagreed.
- `ManagedPythonWorker` now requires nonblank `code` and `message` fields. `ASGI_EXECUTION_FAILED` remains request-specific; `CAPACITY_EXCEEDED` and unknown error codes poison the complete worker generation through the existing active-registry drain, capacity reset, bounded termination, cleanup, and replacement flow.
- Added real scenario `CAPACITY_MISMATCH`. Its E2E-only temporary executable wrapper starts the first Python generation with actual capacity one while preserving the Java readiness expectation of two. The next invocation starts the unmodified production worker with capacity two. No production failure-injection hook was added.

### Commands and observed results

- `cd java-client; mvn -DskipTests clean package`
  Result: PASS - 20 main sources and 3 existing test sources compiled; shaded JAR built; total time 8.266 seconds.
- `cd java-client; mvn test`
  Result: PASS - 8 existing tests, 0 failures, 0 errors, 0 skipped; total time 4.654 seconds.
- First focused `CAPACITY_MISMATCH` run
  Result: E2E harness import-path failure before runtime initialization - the temporary driver could not import `python_runtime`. The E2E-only driver was corrected to include its current application directory; production runtime code was not changed for this issue.
- `docker build --no-cache --progress=plain -t java-fastapi-runtime-poc .`
  Result: PASS - the exact final source built without cache with exit code 0 in 69 seconds. An earlier attempt exported the image but exceeded its 60-second command timeout; it was rerun to obtain an unambiguous successful exit code.
- `docker run --rm java-fastapi-runtime-poc MANAGED_RUNTIME PHASE3_E2E CAPACITY_MISMATCH`
  Result: PASS - Java capacity two versus Python capacity one produced a genuine correlated `CAPACITY_EXCEEDED`; original PID 27 became unhealthy, both active assignments failed without retry and were terminal, `worker-1-g1.sock` was removed, replacement PID 43 became usable, and shutdown removed all runtime resources.
- `docker run --rm java-fastapi-runtime-poc MANAGED_RUNTIME PHASE3_E2E CORRELATED_ERROR`
  Result: PASS - `ASGI_EXECUTION_FAILED` failed one request, three siblings succeeded, PID 28 remained healthy and was reused, and released capacity served a subsequent request.
- `docker run --rm java-fastapi-runtime-poc HTTP E2E`
  Result: PASS - the unchanged real OkHttp/Uvicorn E2E passed.
- `docker run --rm java-fastapi-runtime-poc MANAGED_RUNTIME E2E`
  Result: PASS - the standard managed real-process/UDS/ASGI E2E passed.
- Final Phase 2 capacity-one matrix: `PARALLEL`, `QUEUE`, `IDLE_FAILURE`, `BUSY_FAILURE`, `STARTUP_FAILURE`, `RESTART_EXHAUSTION`, `REQUEST_TIMEOUT`, `UNHEALTHY_FORCE_TERMINATION`, `SHUTDOWN`, and `SHUTDOWN_TIMEOUT`
  Result: PASS - all ten scenarios passed. Observations included four-way overlap in 1,076.542 ms, every bounded queue future terminal, idle PID 31 replaced by PID 59, one non-retried busy failure with replacement PID 63, 87 ms bounded startup failure, bounded restart exhaustion, timeout replacement PID 28 to PID 41, forced unhealthy termination in 875 ms, graceful shutdown in 747 ms, and forced shutdown in 513 ms against one 500 ms deadline.
- Final Phase 3 regression matrix: `SAME_WORKER`, `MULTI_WORKER`, `OUT_OF_ORDER`, `CAPACITY`, `MULTI_ACTIVE_FAILURE`, `MULTI_ACTIVE_TIMEOUT`, `SHUTDOWN`, and `SHUTDOWN_TIMEOUT`
  Result: PASS - same-worker overlap was four in 560.888 ms; multi-worker overlap was six in 530.769 ms; out-of-order mappings were correct; configured active capacity never exceeded two; killed PID 29 failed three requests without retry and was replaced by PID 52; timeout-poisoned PID 27 was replaced by PID 43; graceful shutdown drained six active and failed four queued requests in 359 ms; forced shutdown made all six futures terminal in 522 ms against one 500 ms deadline.
- `docker ps -a --filter ancestor=java-fastapi-runtime-poc --format "{{.ID}} {{.Status}} {{.Names}}"`
  Result: PASS - output was empty after the complete matrix; no test container remained.
- `git diff --check` and unchanged-scope source audit
  Result: PASS - no whitespace errors; HTTP code, FastAPI application, Python runtime and ASGI adapter, public `PythonCallUtil`, protocol framing/version, configuration, CLI, Docker routing, GIL behavior, and CPU/I/O behavior remain unchanged.

### Focused acceptance result

PASS - `CAPACITY_EXCEEDED` now fails the affected request and all sibling assignments exactly once, removes all generation capacity, terminates and cleans the mismatched worker, and restores capacity through the existing restart policy. Normal `ASGI_EXECUTION_FAILED` behavior remains request-specific. No Phase 4 work was started.

---

# Phase 4 - Production Operations Hardening

State: COMPLETE
Date: 2026-08-06
Environment: Windows 10 host with Docker Desktop Linux containers; final image uses Java 17 and Python 3.12

## Implemented

- Added idle-only, bounded PING/PONG health monitoring over each worker's existing UDS connection. One shared scheduler and one outstanding check per current generation detect a stuck idle event loop without consuming business capacity or creating per-check threads.
- Added explicit process-alive, responsive, dispatch-eligible, ready, fully-ready, degraded, saturated, exhausted, and unavailable semantics through `PythonCallUtil.managedRuntimeSnapshot()` and immutable bounded snapshots.
- Added atomic bounded counters/timers, stable failure categories, one bounded last-failure summary, and SLF4J `key=value` lifecycle/health/restart/timeout/shutdown events.
- Replaced inherited worker output with one always-draining, worker-identified, chunk- and rate-bounded combined output reader per process.
- Hardened configuration ranges, overflow checks, UDS path length, normalized path ownership, executable/application validation, POSIX private runtime permissions, shared startup deadlines, readiness cleanup, initialization cancellation, JVM shutdown, worker thread joins, and common-deadline resource cleanup.
- Added direct tini-to-Java signal forwarding for managed containers, the real `Phase4E2ERunner`, Phase 4 container routing, and the Managed Python Runtime operations runbook.
- Updated the Phase 2 replacement E2E wait to use the authoritative Phase 4 readiness snapshot instead of treating OS process appearance as readiness. No production failure-injection hook or new unit test was added.

## Files changed

- `java-client/src/main/java/com/example/baseline/utils/python/PythonCallUtil.java`
- `java-client/src/main/java/com/example/baseline/utils/python/ManagedPythonRuntime.java`
- `java-client/src/main/java/com/example/baseline/utils/python/ManagedPythonWorker.java`
- `java-client/src/main/java/com/example/baseline/utils/python/PythonRuntimeProtocol.java`
- `java-client/src/main/java/com/example/baseline/utils/python/ManagedPythonRuntimeSnapshot.java`
- `java-client/src/main/java/com/example/baseline/utils/python/ManagedPythonFailureCategory.java`
- `java-client/src/main/java/com/example/baseline/utils/config/ApplicationConfig.java`
- `java-client/src/main/resources/application.yml`
- `python-fastapi/python_runtime/worker_runtime.py`
- `java-client/src/main/java/com/example/baseline/e2e/Phase2E2ERunner.java`
- `java-client/src/main/java/com/example/baseline/e2e/Phase4E2ERunner.java`
- `java-client/src/main/java/com/example/baseline/BaselineApplication.java`
- `docker-entrypoint.sh`
- `README.md`
- `docs/codex/OPERATIONS_RUNBOOK.md`
- `docs/codex/PHASE_STATUS.md`

## Commands and observed results

- `cd java-client; mvn -DskipTests clean package`
  Result: PASS - 23 main sources and 3 existing test sources compiled for Java 17; the shaded JAR was built; total time 10.301 seconds.
- `cd java-client; mvn test`
  Result: PASS - 8 existing tests, 0 failures, 0 errors, 0 skipped; total time 6.555 seconds.
- `docker build --no-cache -t java-fastapi-runtime-poc .`
  Result: PASS - the final source built without cache with exit code 0 in 91.9 seconds.
- `docker run --rm java-fastapi-runtime-poc HTTP` and `docker run --rm java-fastapi-runtime-poc HTTP E2E`
  Result: PASS - real OkHttp/Uvicorn completed 1,000/1,000 workload requests in 5,157.796 ms and the existing HTTP lifecycle, headers, validation/non-2xx, and concurrent-caller E2E passed. The final no-cache image HTTP E2E was rerun successfully.
- `docker run --rm java-fastapi-runtime-poc MANAGED_RUNTIME` and `docker run --rm java-fastapi-runtime-poc MANAGED_RUNTIME E2E`
  Result: PASS - four Java-owned CPython workers completed 1,000/1,000 requests in 4,950.181 ms without Uvicorn, followed by the managed lifecycle, validation/non-2xx, and concurrent-caller E2E. The final no-cache image managed E2E was rerun successfully.
- Phase 2 matrix: `PARALLEL`, `QUEUE`, `IDLE_FAILURE`, `BUSY_FAILURE`, `STARTUP_FAILURE`, `RESTART_EXHAUSTION`, `REQUEST_TIMEOUT`, `UNHEALTHY_FORCE_TERMINATION`, `SHUTDOWN`, and `SHUTDOWN_TIMEOUT`
  Result: PASS - all ten real-process scenarios passed. Observations included four-process overlap in 1,191.752 ms; five bounded queue failures with all futures terminal; idle PID 33 replaced by ready PID 64; one non-retried busy failure; bounded zero-child startup failure in 100 ms; bounded exhaustion after three generations; timeout replacement PID 27 to PID 42; forced suspended-worker termination in 930 ms; graceful drain in 744 ms; and forced common-deadline shutdown in 566 ms against a 500 ms deadline.
- Phase 3 matrix: `SAME_WORKER`, `MULTI_WORKER`, `OUT_OF_ORDER`, `CAPACITY`, `MULTI_ACTIVE_FAILURE`, `MULTI_ACTIVE_TIMEOUT`, `CORRELATED_ERROR`, `CAPACITY_MISMATCH`, `SHUTDOWN`, and `SHUTDOWN_TIMEOUT`
  Result: PASS - all ten scenarios passed. One PID overlapped four requests in 612.467 ms; two PIDs overlapped six in 550.413 ms; completion order was `[order-1, order-3, order-2, order-0]` with correct mappings; bounded capacity produced four terminal failures; worker death failed three assignments without retry; transmitted timeout failed three siblings and replaced the PID; correlated ASGI error affected one request only; genuine capacity mismatch poisoned/replaced the worker; graceful shutdown drained six active tasks in 379 ms; forced shutdown terminalized six futures in 541 ms against one 500 ms deadline.
- `MANAGED_RUNTIME PHASE4_E2E IDLE_HEALTH`
  Result: PASS - one PID completed three PING/PONG cycles; `healthSent=3`, `healthSucceeded=3`, business in-flight remained zero, available capacity remained two, and a later business request succeeded. The scenario passed again against the final no-cache image.
- `MANAGED_RUNTIME PHASE4_E2E STUCK_IDLE`
  Result: PASS - real PID 27 was suspended with `SIGSTOP`, one health timeout detected it without a business request, forced termination removed it, PID 44 became ready, and subsequent work succeeded. The scenario passed against the final no-cache image.
- `STALE_PONG` and `BUSY_HEALTH_POLICY`
  Result: PASS - an old-generation PONG did not change capacity/publication; the correct response completed health. No PING was sent during a valid 900 ms active request, health resumed after idle, and the PID remained stable.
- `DEGRADED_VISIBILITY`, `RESTART_EXHAUSTION_VISIBILITY`, and `PARTIAL_STARTUP`
  Result: PASS - killing one of two PIDs exposed `DEGRADED`, healthy service continued, and replacement restored full readiness; one-slot restart exhaustion exposed `UNAVAILABLE` without an infinite loop; controlled partial startup succeeded degraded and restored two ready PIDs with one bounded restart. Partial startup passed again against the final image.
- `HEALTH_SHUTDOWN_RACE` and `SNAPSHOT_METRICS`
  Result: PASS - shutdown with an outstanding delayed health response completed in 1,018 ms with no managed thread remaining; metrics exposed admitted/completed, queue/request timers, health success, worker restart, and bounded `SOCKET_EOF` last-failure context.
- `RESOURCE_REPETITION`
  Result: PASS - three real worker-kill/replacement cycles observed four generations, three restarts, successful requests after every recovery, and clean final process/socket/directory shutdown.
- `MANAGED_RUNTIME PHASE4_E2E SOAK`
  Result: PASS - two observed 5,100-request runs completed with stable four-PID sets, zero pending queue/in-flight work after batches, and clean shutdown. Final-image evidence: all worker FD counts remained exactly 8; per-worker RSS remained unchanged at 42,836-42,932 KiB.
- Detached `PARENT_TERMINATION` containers followed by `docker kill --signal=TERM` and `docker kill --signal=KILL`
  Result: PASS - `docker top` showed `tini -> Java -> two CPython workers`; SIGTERM removed the container/process namespace in 1,467 ms through the JVM hook, and SIGKILL removed it in 1,316 ms through Docker container teardown.
- Final cleanup and source audit using `docker ps`, `git diff --name-only`, `git diff --check`, and `rg`
  Result: PASS - no test container remained; FastAPI `app/main.py`, `app/models.py`, `asgi_adapter.py`, `HttpUtil`, and `RestProcessingClient` had no diff; no CPU/I/O split, GIL manipulation, automatic business retry, unbounded health executor, or request-body logging was introduced.

## Acceptance gates

- PASS - clean Java package and all eight existing tests succeed; no unit tests were added.
- PASS - existing HTTP/OkHttp/Uvicorn and managed UDS/ASGI modes both pass and rollback remains the `HTTP` argument only.
- PASS - idle event-loop responsiveness is monitored through bounded generation-correlated PING/PONG without business-capacity consumption or per-check threads.
- PASS - busy workers are not health-checked, avoiding false failures during CPU-heavy or delayed active execution.
- PASS - stale health responses cannot revive or republish an old generation; one outstanding health record is enforced per worker.
- PASS - snapshots, bounded metrics, typed failures, structured logs, and worker-identified bounded output provide operational evidence without payload exposure.
- PASS - all/partial/zero-worker startup, degraded recovery, restart exhaustion, crash, timeout, saturation, and capacity mismatch remain bounded and observable.
- PASS - health scheduling stops before drain; no replacement begins during close; every future is terminal; one shared deadline governs shutdown and thread/process/socket cleanup.
- PASS - soak, repetition, SIGTERM, and SIGKILL container tests leave no CPython descendant, UDS socket, runtime directory, managed thread, or container.
- PASS - FastAPI business application, ASGI adapter, HTTP utility, REST client, CPython GIL behavior, and unified request path remain unchanged.

## Remaining limitations

- Worker count and per-worker in-flight capacity remain fixed configuration; autoscaling is not implemented.
- Health checks intentionally cover idle workers only. Active CPU-heavy or stuck requests are detected by their existing request deadline.
- A transmitted timeout or ambiguous transport failure poisons the full worker generation and fails siblings without retry.
- Snapshots and metrics are in-process diagnostics; no Prometheus, JMX, OpenTelemetry, or external health server was added.
- `SIGKILL` cannot execute JVM cleanup; orphan prevention for that signal relies on the documented Docker/tini container process namespace.
- Managed UDS E2E was executed in Linux containers rather than on the Windows host.
- The unchanged FastAPI/Pydantic combination continues to emit existing alias warnings.

## Final statement

Phase 4 complete

---

## Focused Phase 4 health and shutdown deadline corrections

Date: 2026-08-06

- PONG timeout ownership now begins only after the complete PING frame has been written to the worker UDS. A queued health item has no response deadline.
- An already-transmitted PING remains timeout-eligible even when business work is assigned afterward. The idle requirement applies only when creating a new health check.
- Pool shutdown now reserves a bounded tail of its single absolute deadline for forced process termination and thread/output joins. Worker cleanup no longer creates a separate 250 ms join deadline, and forced termination is reported exactly once per generation.
- Added the real-process `HEALTH_TIMEOUT_WITH_ACTIVE` Phase 4 scenario; no production failure-injection hook or unit test was added.

### Commands and observed results

- `cd java-client; mvn -DskipTests clean package`
  Result: PASS - 23 main sources and 3 existing test sources compiled; shaded JAR built; final total time 9.161 seconds.
- `cd java-client; mvn test`
  Result: PASS - 8 existing tests, 0 failures, 0 errors, 0 skipped; final total time 4.699 seconds.
- `docker build -t java-fastapi-runtime-poc .`
  Result: PASS - final focused source image built successfully with exit code 0.
- `docker run --rm java-fastapi-runtime-poc MANAGED_RUNTIME PHASE4_E2E HEALTH_TIMEOUT_WITH_ACTIVE`
  Result: PASS - PING was transmitted before business assignment; the delayed PONG timed out while one business request was active, that future became terminal, PID 27 was removed, replacement PID 43 became ready and served subsequent work, and final cleanup reported `cleanupFailures=0`.
- `docker run --rm java-fastapi-runtime-poc MANAGED_RUNTIME PHASE4_E2E IDLE_HEALTH` and `BUSY_HEALTH_POLICY`
  Result: PASS - three normal PING/PONG cycles succeeded without consuming business capacity; no new PING was created during active work and health resumed after idle.
- `docker run --rm java-fastapi-runtime-poc MANAGED_RUNTIME PHASE4_E2E HEALTH_SHUTDOWN_RACE`
  Result: PASS - shutdown with an outstanding delayed health response completed in 998 ms with no remaining managed thread, worker process, socket, or runtime directory.
- `docker run --rm java-fastapi-runtime-poc MANAGED_RUNTIME PHASE3_E2E SHUTDOWN`
  Result: PASS - six active requests drained, four queued requests failed, new calls were rejected, all futures became terminal, and both worker PIDs were removed in 372 ms.
- `docker run --rm java-fastapi-runtime-poc MANAGED_RUNTIME PHASE3_E2E SHUTDOWN_TIMEOUT`
  Result: PASS - three consecutive final-image runs made all six active futures terminal in 279, 280, and 272 ms against one 500 ms deadline. Every run reported two forced terminations, zero cleanup failures, and removal of both PIDs and owned UDS resources.
- `docker run --rm java-fastapi-runtime-poc HTTP E2E`
  Result: PASS - unchanged OkHttp/Uvicorn lifecycle, validation/non-2xx, and concurrent-caller behavior passed.
- `docker run --rm java-fastapi-runtime-poc MANAGED_RUNTIME E2E`
  Result: PASS - four real Java-owned CPython workers passed the standard UDS/ASGI lifecycle and concurrent-caller regression; shutdown reported zero cleanup failures.
- `docker ps -a --filter ancestor=java-fastapi-runtime-poc --format "{{.ID}} {{.Status}} {{.Names}}"`
  Result: PASS - output was empty; no test container remained.

### Focused acceptance result

PASS - PONG timing is transmission-based, later business activity cannot suppress an outstanding health timeout, and all forced-cleanup waits use the one pool shutdown deadline. HTTP and FastAPI application code remain unchanged.
