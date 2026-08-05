# Codex Start Here

The repository is prepared so the user can issue only:

```text
Start Phase 1
```

Codex must then follow `AGENTS.md` and the phase documents without needing a second implementation prompt.

## Current system

```text
Java application
  -> RestProcessingClient
  -> HttpUtil / OkHttp3
  -> Uvicorn
  -> existing FastAPI application
```

## Required final dual-mode system

```text
Java application
  -> RestProcessingClient
  -> PythonCallUtil (utility facade)
       |-- HTTP
       |     -> HttpUtil / OkHttp3
       |     -> Uvicorn
       |     -> existing FastAPI application
       |
       `-- MANAGED_RUNTIME
             -> Java Managed Python Runtime
             -> Unix Domain Socket
             -> Python runtime adapter
             -> ASGI
             -> existing FastAPI application
```

Examples after the relevant phase is implemented:

```bash
java -jar java-client/target/java-client-1.0.0.jar HTTP
java -jar java-client/target/java-client-1.0.0.jar MANAGED_RUNTIME
```

One mode is selected for one application run. Business request code must not contain HTTP-versus-managed branching.

## Document order

1. `AGENTS.md`
2. `docs/codex/ARCHITECTURE_AND_RULES.md`
3. `docs/codex/PHASES.md`
4. `docs/codex/E2E_AND_STATUS.md`
5. `docs/codex/PHASE_STATUS.md`

The coding agent must inspect the actual source after reading these documents and must update the status file only with observed results.
