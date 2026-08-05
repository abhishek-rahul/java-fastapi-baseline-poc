# Managed Python Runtime Architecture and Engineering Rules

## 1. Objective

Replace the optional internal Java-to-Python HTTP hop with a Java-managed pool of long-lived CPython processes while preserving the existing FastAPI application and retaining HTTP as a fallback and comparison path.

This is not CPython embedding. Python executes in child CPython processes started by Java.

## 2. Fixed architecture

### HTTP mode

```text
A / Java caller
  -> RestProcessingClient
  -> PythonCallUtil
  -> HTTP implementation
  -> existing HttpUtil / OkHttp3
  -> Uvicorn
  -> existing FastAPI application
```

### Managed runtime mode

```text
A / Java caller
  -> RestProcessingClient
  -> PythonCallUtil
  -> managed-runtime implementation
  -> Java Managed Python Runtime
  -> UDS framed messages
  -> Python runtime adapter
  -> ASGI app(scope, receive, send)
  -> existing FastAPI application
```

## 3. Mode boundary

The Java application parses the first command-line argument as an enum:

- `HTTP`
- `MANAGED_RUNTIME`

Invalid values fail fast with an actionable message. A documented default may remain `HTTP` for backward compatibility, but the E2E commands must always pass the mode explicitly.

`RestProcessingClient` must call one common utility API and must not import OkHttp response types after the common facade is introduced.

Recommended minimal Java boundary:

```text
utils/python/
  PythonCallUtil
  PythonCallMode
  PythonCallRequest
  PythonCallResponse
  PythonCallExecutor
  http/HttpPythonCallExecutor
  runtime/ManagedRuntimePythonCallExecutor
```

Names may be adjusted to fit the repository, but the responsibilities and layering must remain. Avoid a large framework or dependency-injection container for this POC.

The common response needs only what the caller actually uses, such as status code, headers when required, and raw response body. Do not duplicate business DTOs inside the runtime.

## 4. Python application boundary

Python runtime code must be added in a sibling package, for example:

```text
python-fastapi/
  app/                 # existing application: do not change
  python_runtime/      # managed runtime implementation
```

The runtime imports the configured object, currently `app.main:app`, and invokes:

```python
await app(scope, receive, send)
```

It must preserve normal FastAPI routing, Pydantic validation, dependencies, middleware, exception handling, status codes, headers, and lifespan behavior supported by the implemented phase.

Directly importing and calling `process()` or another endpoint function is forbidden.

## 5. Concurrency and the GIL

The CPython GIL remains unchanged.

Three different forms of concurrency must not be confused:

1. Java request concurrency: existing Java executor threads submit multiple requests concurrently.
2. Process parallelism: multiple CPython worker processes can run on different CPU cores.
3. ASGI async concurrency: Phase 3 allows multiple in-flight ASGI tasks in one worker event loop, mainly benefiting awaitable I/O.

A Java synchronized block may protect a very short lifecycle or state transition, but no lock may cover the full request send/wait/receive path. Such a lock would serialize all callers independently of the GIL.

Phase 3 must have one unified execution path. There are no `CPU`, `IO`, `SAFE`, or route-policy profiles. The same pool uses configured worker count and configured maximum in-flight requests per worker.

## 6. Request correlation

Each managed request carries a unique request ID. Java stores a pending completion object by ID before sending the frame. The Python response repeats the same ID. A dedicated response-reading path completes the matching pending request, even when responses arrive in a different order.

Rules:

- Never reuse an ID while pending.
- Never complete one request twice.
- Remove pending entries on success, error, timeout, connection loss, and shutdown.
- Never assume response order equals request order.
- A worker loss fails all pending requests assigned to that worker with a deterministic error.

## 7. Protocol and transport

Use Unix Domain Sockets first. Keep protocol encoding independent of UDS so it can be tested and replaced later without changing the application boundary.

Minimum message concepts by the phase that needs them:

- readiness handshake
- request
- response
- error
- ping/pong
- drain/shutdown

Use length-prefixed framing with explicit limits. Send JSON metadata and raw UTF-8 JSON body bytes; do not use Java serialization or Python pickle.

## 8. Lifecycle ownership

Java owns:

- worker process creation with `ProcessBuilder`
- unique UDS paths
- startup timeout and readiness
- dispatch and capacity accounting
- process exit detection
- graceful drain and forced cleanup
- removal of stale socket files created by this runtime

Python owns inside each worker:

- one long-lived asyncio event loop
- importing the FastAPI app
- ASGI lifespan startup and shutdown
- UDS request handling
- ASGI request task creation in Phase 3
- emitting responses with the original request ID

## 9. Simplicity constraints

Do not add these unless the current phase explicitly requires them:

- TCP transport
- autoscaling
- gRPC, Py4J, JEP, GraalPy, or JNI
- route classification
- plugin frameworks
- reflection-based registries
- custom annotation systems
- generic retry engines
- streaming, WebSockets, or server-sent events
- database or external service abstractions

## 10. Compatibility and fallback

The existing HTTP implementation remains functional. Mode selection changes only the invocation implementation. The same logical request must produce equivalent status and JSON response through both modes for supported routes.

Managed mode must not require Uvicorn or port 8000. HTTP mode continues to require them.
