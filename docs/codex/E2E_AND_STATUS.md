# End-to-End Execution and Status Rules

## 1. E2E means real boundaries

A phase E2E check must use:

- the built Java JAR
- real Java concurrency configured by the application
- real CPython child processes in managed mode
- real Unix Domain Sockets in managed mode
- the real `app.main:app` FastAPI application invoked through ASGI
- real Uvicorn and OkHttp3 in HTTP mode

Mocks, direct endpoint calls, isolated Python functions, or protocol-only programs do not satisfy a phase gate.

## 2. Environment discovery

Before implementation, Codex must inspect available Java, Maven, Python, Docker, OS, and UDS support. It may adapt commands to the environment but must record the exact versions and commands used.

If the current environment cannot execute a mandatory E2E scenario, Codex must still complete code that can run in the documented target environment, provide the exact unrun command, and mark the phase blocked rather than complete.

## 3. Required execution discipline

For every requested phase:

1. Capture the pre-change HTTP baseline command and result.
2. Build from source; do not rely on the JAR already under `target`.
3. Run the new managed mode scenario.
4. Run HTTP mode again to detect regression.
5. Run the phase's failure/concurrency scenarios.
6. Inspect processes and UDS files after shutdown.
7. Save concise evidence in `PHASE_STATUS.md`.

Do not use only a successful exit code as proof. Record the relevant request count, success count, worker count, in-flight configuration, wall-clock observation, and failure behavior.

## 4. Suggested baseline commands

These are starting points; Codex must adjust them to actual implementation and platform.

Python HTTP server:

```bash
cd python-fastapi
python3.12 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements.txt
python -m uvicorn app.main:app --host 127.0.0.1 --port 8000
```

Java package without adding/running new unit tests:

```bash
cd java-client
mvn -DskipTests clean package
```

Mode runs:

```bash
java -jar target/java-client-1.0.0.jar HTTP
java -jar target/java-client-1.0.0.jar MANAGED_RUNTIME
```

Container baseline when Docker is available:

```bash
docker build --no-cache -t java-fastapi-runtime-poc .
docker run --rm java-fastapi-runtime-poc HTTP
```

Codex must update the Docker entrypoint in the appropriate phase so mode and relevant runtime configuration are explicit and both modes are runnable. Do not invent a container acceptance result when Docker is unavailable.

## 5. Concurrency evidence

Use values that make behavior visible, such as multiple Java threads, more requests than workers, and delay durations long enough to distinguish serial from overlapping execution.

Evidence should include at least one of:

- worker-ID/PID logs showing overlapping active intervals
- request start/end timestamps showing overlap
- wall-clock comparison against a calculated serial lower bound

Do not assert parallel or async execution solely because multiple Java futures were created.

## 6. Phase status format

Update `docs/codex/PHASE_STATUS.md` using this structure:

```text
Phase:
State: NOT_STARTED | IN_PROGRESS | BLOCKED | COMPLETE
Date:
Environment:

Repository findings:
- ...

Implemented:
- ...

Files changed:
- ...

Commands executed:
- command
  Result: ...

E2E gates:
- PASS/FAIL/NOT RUN — gate and evidence

Known limitations:
- ...

Deferred work:
- ...

Final statement:
- Phase N complete
  or
- Phase N is not complete because ...
```

Never replace failed or unrun evidence with planned behavior.
