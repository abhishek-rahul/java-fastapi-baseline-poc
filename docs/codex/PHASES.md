# Phase-by-Phase Delivery Plan

Each phase must end with runnable code and observed E2E evidence. Implement only the phase requested by the user.

## Phase 1 — Dual-Mode Compatibility Foundation

### Goal

Introduce the clean Java utility boundary and prove that one Java-managed CPython worker can invoke the unchanged FastAPI application directly through ASGI over UDS. Keep HTTP mode working.

### Required implementation

1. Scan the repository and record the current call path and lifecycle before editing.
2. Add command-line mode parsing for `HTTP` and `MANAGED_RUNTIME`.
3. Add `PythonCallUtil` and the smallest common request/response boundary needed by `RestProcessingClient`.
4. Move current OkHttp use behind the HTTP implementation without changing `HttpUtil` behavior unnecessarily.
5. Add one Java-managed CPython worker process for managed mode.
6. Add a Python runtime package outside `app` that imports `app.main:app` and invokes it through ASGI.
7. Add a minimal bounded UDS protocol with readiness, request, response, error, and shutdown.
8. Preserve the synchronous `RestProcessingClient.process(...)` contract. Internals may use pending completions.
9. Implement deterministic startup, timeout, shutdown, and UDS cleanup for the single worker.
10. Keep the existing FastAPI application files unchanged.

### Deliberate Phase 1 limits

- Exactly one managed worker.
- At most one active Python request in that worker.
- No worker pool, restart loop, health loop, multiple in-flight tasks, route profiles, TCP, or production metrics framework.
- Java callers may submit concurrently; the runtime may queue them in a bounded way. Do not globally lock the whole call path.

### Mandatory E2E gate

- Java packages successfully.
- HTTP mode starts Uvicorn and completes the configured workload.
- Managed mode starts Python itself, does not start Uvicorn, and completes the same workload.
- The same request produces equivalent status and business response fields in both modes.
- Validation/non-200 behavior is preserved for at least one invalid request executed through a real end-to-end path.
- Managed mode can be started and stopped repeatedly without an orphan child process or stale UDS file.
- `python-fastapi/app/main.py` and `python-fastapi/app/models.py` have no runtime-specific changes.

### Phase 1 completion output

Update `docs/codex/PHASE_STATUS.md` with changed files, exact commands, observed results, known limits, and the explicit statement `Phase 1 complete` only when all gates pass. Then stop.

---

## Phase 2 — Reliable Multi-Process Worker Pool

### Goal

Provide actual process-level parallelism using a configurable fixed pool of CPython workers. Keep one active request per worker for a simple, correctness-first model.

### Required implementation

1. Replace single-worker ownership with a fixed worker pool using the Phase 1 interfaces.
2. Add clear worker states and transition rules: starting, ready, busy, draining, unhealthy, stopped.
3. Dispatch concurrent Java calls to available ready workers without a global request lock.
4. Use a bounded admission queue and queue-wait timeout.
5. Track each request's assigned worker and release capacity exactly once.
6. Detect process exit and socket EOF.
7. Fail affected pending requests deterministically when a worker is lost.
8. Replace failed workers using bounded restart attempts and backoff.
9. Add readiness and lightweight ping/pong health checks only as needed for reliable operation.
10. Add graceful pool drain and forced termination after a configured timeout.

### Deliberate Phase 2 limits

- One active request per worker.
- No multiple in-flight ASGI tasks inside one worker.
- No CPU/IO route separation.
- No TCP, autoscaling, or automatic business-request retry.

### Mandatory E2E gate

- Configure at least four workers and enough Java threads/requests to keep them active.
- Demonstrate overlapping work in multiple distinct Python worker processes and materially lower wall-clock time than one-worker serial execution.
- Demonstrate that many Java requests are submitted concurrently rather than one by one.
- Kill one idle worker; the pool detects and replaces it.
- Kill one worker during a request; that request completes with a deterministic failure and the pool remains usable.
- Fill the bounded queue; excess work fails or times out predictably rather than waiting forever.
- Shutdown leaves no child processes or UDS files owned by the runtime.
- HTTP mode still completes its E2E workload unchanged.

### Phase 2 completion output

Update the phase status with commands and evidence. Write `Phase 2 complete` only after all gates pass, then stop.

---

## Phase 3 — Unified Async ASGI Concurrency

### Goal

Allow a configurable number of concurrent in-flight ASGI requests per Python worker, similar to the async request overlap provided by Uvicorn and FastAPI, while retaining multiple worker processes.

There is exactly one managed execution path. Do not create CPU and I/O modes, route policies, separate pools, or automatic workload classification.

### Required implementation

1. Add configurable `maxInFlightPerWorker` to the existing single pool.
2. Permit Java to send multiple requests to the same ready worker up to its capacity.
3. Maintain a thread-safe pending-request registry keyed by request ID.
4. Use a dedicated response-reading mechanism that supports responses arriving in any order.
5. On Python, create one asyncio task per accepted request and retain task references until completion.
6. Limit accepted tasks with explicit per-worker capacity; do not create unbounded tasks.
7. Dispatch to a ready worker with available capacity, preferably the least in-flight worker using simple deterministic tie-breaking.
8. Clean up request state and capacity exactly once for success, error, timeout, cancellation, worker loss, and shutdown.
9. Keep the synchronous Java facade: each Java caller waits only for its own correlated completion.
10. Keep CPython and its GIL unchanged. Process parallelism still comes from multiple workers; in-worker overlap comes from ASGI/asyncio.

### Mandatory E2E gate

- With one worker and `maxInFlightPerWorker` greater than one, multiple delay-based FastAPI requests overlap and complete substantially faster than serial execution.
- Use requests with different delays so responses return out of submission order; every Java caller receives the response matching its own request ID and payload.
- Run concurrency higher than one worker's limit; in-flight counts never exceed configuration and overflow uses the bounded queue.
- Repeat concurrency runs to show no steadily growing Java pending registry, Python task registry, file descriptors, or child-process count.
- A worker failure with several in-flight requests completes all affected Java calls deterministically and restores configured capacity after replacement.
- HTTP mode remains available and behaviorally equivalent.
- Source contains no CPU/IO route profiles and no attempt to change the GIL.

### Phase 3 completion output

Update status with concurrency configuration, wall-clock observations, out-of-order proof, cleanup evidence, and known limitations. Write `Phase 3 complete` only after all gates pass, then stop.

---

## Phase 4 — Production Hardening and Reversible Rollout

### Goal

Make the managed runtime observable, bounded, secure, cleanly deployable, and safely reversible while keeping the implementation focused.

### Required implementation

1. Validate all runtime configuration before starting a worker.
2. Add structured lifecycle and request logs with request ID and worker ID; do not log bodies by default.
3. Add lightweight metrics or counters needed to distinguish queue wait, execution time, worker health, restarts, timeouts, and in-flight work. Use the repository's existing stack; do not add an observability platform only for this POC.
4. Harden startup, deadlines, queue timeout, worker-loss handling, drain, forced cleanup, and parent shutdown behavior.
5. Use a private UDS directory and safe path/permission handling appropriate to the target OS/container.
6. Capture worker stdout/stderr with bounded behavior and worker identity.
7. Ensure configuration and command examples work in local execution and the repository container packaging.
8. Preserve immediate mode-level rollback to `HTTP` with no business-code change.
9. Add an operator runbook for startup failures, saturation, worker crash loops, timeouts, shutdown, and HTTP rollback.
10. Remove temporary debugging, unused code, duplicate abstractions, and phase-only scaffolding.

### Mandatory E2E gate

- Clean-build and run both modes from documented commands.
- Managed mode handles representative concurrency, timeout, saturation, worker crash, replacement, and graceful shutdown scenarios.
- Repeated start/stop and a soak run show stable process count, pending-request count, Python task count, UDS files, and memory trend within reasonable test noise.
- Abrupt Java termination does not leave persistent worker processes in the documented deployment environment.
- HTTP rollback is demonstrated using only the mode argument/configuration, not a code change.
- FastAPI application code remains runtime-agnostic.
- The final code review finds no unbounded queue, request-wide global lock, CPU/IO split, GIL manipulation, unnecessary dependency, unused variable, or speculative framework.

### Phase 4 completion output

Update status with complete runbook and E2E evidence. Write `Phase 4 complete` only when all gates pass, then stop.

---

## Deferred test hardening — not part of Phases 1-4

Do not begin this work until the user explicitly requests it after accepting all four phases.

At that time, add focused unit and component tests for protocol framing, configuration validation, state transitions, capacity accounting, correlation, error mapping, and cleanup races. Do not pre-build this test framework during the implementation phases.
