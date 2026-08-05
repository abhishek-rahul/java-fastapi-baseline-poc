# Managed Python Runtime Operations Runbook

## Launch and rollback

Build the container:

```powershell
docker build --no-cache -t java-fastapi-runtime-poc .
```

Run the Java-managed CPython pool without Uvicorn:

```powershell
docker run --rm java-fastapi-runtime-poc MANAGED_RUNTIME
```

Rollback requires no source or image change. Select the existing OkHttp/Uvicorn path:

```powershell
docker run --rm java-fastapi-runtime-poc HTTP
```

## Readiness and capacity

The managed runtime is ready when at least one current worker generation is responsive.
It is fully ready when all configured slots are responsive. `DEGRADED` means at least one
worker can serve while another slot is starting, restarting, or exhausted. `UNAVAILABLE`
means no responsive worker exists. Healthy workers with all capacity in use are saturated,
not unavailable; inspect queue depth and available capacity separately.

`PythonCallUtil.managedRuntimeSnapshot()` provides a bounded immutable view containing pool
state, slot generations/PIDs, queue and in-flight counts, restart/health state, aggregate
counters and timers, and one bounded last-failure summary. It returns empty in HTTP mode or
when the managed executor is not published.

## Health monitoring

Health monitoring uses PING/PONG on each worker's existing UDS connection. Only idle workers
are pinged, so CPU-heavy or delayed active requests remain governed by the request timeout.
Defaults are a 5-second interval, 1-second timeout, and 5-second startup grace. A missing PONG
poisons that generation and invokes the existing bounded slot restart policy. Health checks do
not consume business in-flight capacity.

Useful structured events include `managed_python_pool_ready`,
`managed_python_pool_degraded`, `managed_python_pool_unavailable`,
`managed_python_worker_unhealthy`, `managed_python_health_check_failed`,
`managed_python_worker_restart_scheduled`, and `managed_python_pool_shutdown_completed`.
DEBUG logging adds request and PING/PONG correlation without payload bodies.

## Failure response

- Startup failure: check the normalized application directory, executable, UDS path length,
  private-directory permission, and readiness failure category. Zero ready workers makes
  initialization fail and cleans all partial resources.
- Saturation: inspect queue depth, maximum in-flight capacity, queue-full observations, and
  queue-timeout counters. Increasing capacity is an operator configuration decision; requests
  are never automatically retried.
- Crash or idle hang: confirm the unhealthy event, stable slot ID, old generation/PID, restart
  backoff, and replacement generation/PID. An exhausted slot remains visible and does not loop.
- Request timeout: pre-transmission timeout releases only that reservation. A transmitted or
  ambiguous timeout poisons the worker and fails siblings without retry.
- Cleanup failure: treat a surviving process or owned UDS path as an operational error. Stop
  the Java/container process and verify the private runtime directory before restarting.

## Shutdown and parent termination

Normal close and `SIGTERM` use one application-wide shutdown deadline: admission stops, queued
work fails, health scheduling stops, workers drain active work, shutdown acknowledgements are
awaited, and unfinished workers are forcibly terminated. Sockets and the owned runtime directory
are then removed. The registered JVM shutdown hook uses this same idempotent path.

`SIGKILL` cannot execute Java cleanup. In the documented container deployment, Docker and tini
terminate the entire container process namespace, so CPython descendants cannot outlive the
removed container. Validate this with the `PARENT_TERMINATION` Phase 4 scenario and `docker top`
before/after termination.

## E2E diagnostics

Run an individual Phase 4 scenario:

```powershell
docker run --rm java-fastapi-runtime-poc MANAGED_RUNTIME PHASE4_E2E IDLE_HEALTH
```

Available scenarios are `IDLE_HEALTH`, `STUCK_IDLE`, `STALE_PONG`, `BUSY_HEALTH_POLICY`,
`DEGRADED_VISIBILITY`, `RESTART_EXHAUSTION_VISIBILITY`, `PARTIAL_STARTUP`,
`HEALTH_SHUTDOWN_RACE`, `SNAPSHOT_METRICS`, `SOAK`, `PARENT_TERMINATION`, and
`RESOURCE_REPETITION`.

After testing, confirm there are no runtime directories beneath the configured UDS parent and
no remaining containers:

```powershell
docker ps -a --filter ancestor=java-fastapi-runtime-poc
```
