# Codex Repository Instructions

This file is the source of truth for coding agents working in this repository.

## Command contract

When the user says `Start Phase N`:

1. Read this file, `CODEX_START_HERE.md`, `docs/codex/ARCHITECTURE_AND_RULES.md`, `docs/codex/PHASES.md`, and `docs/codex/E2E_AND_STATUS.md`.
2. Inspect the complete repository before editing. Do not assume the documentation perfectly matches the code.
3. Execute only the requested phase. Do not start or partially implement a later phase.
4. Keep both execution modes working:
   - `HTTP`: OkHttp3 -> Uvicorn -> existing FastAPI application.
   - `MANAGED_RUNTIME`: Java managed runtime -> UDS -> Python runtime adapter -> ASGI -> existing FastAPI application.
5. Produce runnable code, run the phase E2E checks, record evidence in `docs/codex/PHASE_STATUS.md`, then stop.
6. If an E2E check fails, diagnose and fix it within the requested phase. Do not mark the phase complete while any mandatory gate is failing.

## Non-negotiable implementation rules

- Use the name **Managed Python Runtime**. Do not introduce any alternate feature name in package, class, file, configuration, or protocol names.
- Do not modify, disable, bypass, or attempt to remove the CPython GIL.
- Do not create separate CPU and I/O execution paths, route profiles, or worker pools.
- Phase 3 has one unified ASGI execution path with configurable concurrent in-flight requests.
- Do not place a Java global lock around request submission, response waiting, socket I/O, or complete invocation execution.
- The existing FastAPI application remains unchanged and is called at the ASGI application boundary. Never call endpoint functions directly.
- The managed mode must not start Uvicorn and must not make an internal HTTP request.
- The HTTP/OkHttp3 mode must remain available throughout all phases.
- Runtime selection is made from the Java command-line mode argument: `HTTP` or `MANAGED_RUNTIME`.
- Managed-runtime implementation belongs behind a Java utility facade under `utils`; Python runtime code belongs outside `python-fastapi/app`.
- Java owns Python process startup, readiness, lifecycle, shutdown, and worker selection.
- Keep queues, pending requests, frame sizes, timeouts, and in-flight counts bounded.
- Do not add automatic retries for requests whose execution state is ambiguous.
- Prefer Java 17 and Python 3.12 standard-library facilities. Add a dependency only when it removes substantial risk and document why.

## Code quality rules

- Make the smallest coherent change that satisfies the current phase.
- Keep code simple, explicit, easy to read, production-oriented, and extensible at defined boundaries.
- Follow single-responsibility, dependency inversion at the mode boundary, clear lifecycle ownership, fail-fast validation, and deterministic cleanup.
- Avoid speculative abstractions, duplicate models, unused variables, placeholder classes, dead code, commented-out alternatives, and future-phase scaffolding.
- Do not silently rename existing business DTOs or alter response semantics.
- Preserve errors with useful context, but do not log payload bodies or secrets by default.
- Use precise names; avoid generic names such as `Manager`, `Helper`, or `Handler` unless the responsibility truly matches.

## Testing policy for the current delivery

- Do not add new unit tests in Phases 1-4.
- Do not delete existing tests.
- Mandatory acceptance is phase-specific end-to-end execution using real Java, real CPython processes, real UDS in managed mode, and real Uvicorn/OkHttp in HTTP mode.
- Package Java with tests skipped when necessary: `mvn -DskipTests clean package`.
- Unit and component test expansion is deferred until the four E2E phases are accepted and the user explicitly requests test hardening.

## Stop conditions

Stop after the requested phase report. Do not continue to another phase. Report blockers honestly; never claim an E2E command was run when it was not.
