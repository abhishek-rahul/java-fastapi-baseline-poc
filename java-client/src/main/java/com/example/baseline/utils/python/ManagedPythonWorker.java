package com.example.baseline.utils.python;

import com.example.baseline.utils.config.ApplicationConfig.ManagedPythonRuntimeConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

final class ManagedPythonWorker {
    private static final Logger LOGGER = LoggerFactory.getLogger(ManagedPythonWorker.class);
    private static final long WRITER_POLL_MILLIS = 25;
    private static final String ASGI_EXECUTION_FAILED = "ASGI_EXECUTION_FAILED";
    private static final String CAPACITY_EXCEEDED = "CAPACITY_EXCEEDED";

    static final class StartupException extends IOException {
        private final ManagedPythonFailureCategory category;

        private StartupException(
                ManagedPythonFailureCategory category, String message, Throwable cause) {
            super(message, cause);
            this.category = category;
        }

        ManagedPythonFailureCategory category() {
            return category;
        }
    }

    interface Listener {
        void onCapacityAvailable(ManagedPythonWorker worker);

        void onUnhealthy(
                ManagedPythonWorker worker,
                ManagedPythonFailureCategory category,
                Exception failure);

        void onRequestTerminal(
                ManagedPythonWorker worker,
                Assignment assignment,
                boolean successful,
                ManagedPythonFailureCategory category);

        void onHealthCheckSent(ManagedPythonWorker worker, String healthCheckId);

        void onHealthCheckSucceeded(ManagedPythonWorker worker, String healthCheckId, long latencyNanos);

        void onForcedTermination(ManagedPythonWorker worker);

        void onCleanupFailure(ManagedPythonWorker worker, String message, Exception failure);
    }

    private interface OutboundItem {
    }

    static final class Assignment implements OutboundItem {
        private final PythonCallRequest request;
        private final CompletableFuture<PythonCallResponse> completion;
        private long requestDeadlineNs;
        private long assignedAtNs;
        private AssignmentState state = AssignmentState.QUEUED;

        Assignment(PythonCallRequest request, CompletableFuture<PythonCallResponse> completion) {
            this.request = request;
            this.completion = completion;
        }

        PythonCallRequest request() {
            return request;
        }

        CompletableFuture<PythonCallResponse> completion() {
            return completion;
        }

        long assignedAtNs() {
            return assignedAtNs;
        }
    }

    private record HealthPing(String healthCheckId, int generation, long workerPid) implements OutboundItem {
    }

    private record OutstandingHealth(String healthCheckId, long startedAtNs, long deadlineNs) {
    }

    private enum WorkerState {
        RUNNING,
        DRAINING,
        UNHEALTHY,
        STOPPED
    }

    private enum AssignmentState {
        QUEUED,
        TRANSMITTING,
        TRANSMITTED,
        TERMINAL
    }

    private final ManagedPythonRuntimeConfig config;
    private final String workerId;
    private final int generation;
    private final Path socketPath;
    private final Process process;
    private final long workerPid;
    private final SocketChannel channel;
    private final ProcessOutputCapture outputCapture;
    private final Listener listener;
    private final Object stateLock = new Object();
    private final ArrayBlockingQueue<OutboundItem> outboundQueue;
    private final Map<String, Assignment> activeAssignments = new HashMap<>();
    private final AtomicBoolean failurePublished = new AtomicBoolean();
    private final AtomicBoolean cleanupStarted = new AtomicBoolean();
    private final CountDownLatch shutdownAcknowledged = new CountDownLatch(1);
    private final CountDownLatch stopped = new CountDownLatch(1);
    private final Thread writerThread;
    private final Thread responseReader;
    private final long startedAtNs = System.nanoTime();
    private final AtomicLong healthSequence = new AtomicLong();
    private volatile WorkerState state = WorkerState.RUNNING;
    private volatile long shutdownDeadlineNs = Long.MAX_VALUE;
    private int inFlightCount;
    private boolean availabilityPublished;
    private OutstandingHealth outstandingHealth;
    private long lastResponsiveNs = System.nanoTime();
    private long lastResponsiveEpochMillis = System.currentTimeMillis();
    private long lastHealthLatencyNanos;

    private ManagedPythonWorker(
            ManagedPythonRuntimeConfig config,
            String workerId,
            int generation,
            Path socketPath,
            Process process,
            SocketChannel channel,
            ProcessOutputCapture outputCapture,
            Listener listener) {
        this.config = config;
        this.workerId = workerId;
        this.generation = generation;
        this.socketPath = socketPath;
        this.process = process;
        this.workerPid = process.pid();
        this.channel = channel;
        this.outputCapture = outputCapture;
        this.listener = listener;
        this.outboundQueue = new ArrayBlockingQueue<>(config.maxInFlightPerWorker() + 1, true);
        this.writerThread = new Thread(this::writerLoop,
                "managed-python-writer-" + workerId + "-g" + generation);
        this.responseReader = new Thread(this::responseLoop,
                "managed-python-response-" + workerId + "-g" + generation);
        this.writerThread.start();
        this.responseReader.start();
    }

    static ManagedPythonWorker start(
            ManagedPythonRuntimeConfig config,
            Path runtimeDirectory,
            String workerId,
            int generation,
            Listener listener,
            long startupDeadlineNs) throws Exception {
        Path applicationDirectory = Path.of(config.applicationDirectory());
        Path socketPath = runtimeDirectory.resolve(workerId + "-g" + generation + ".sock");
        Process process = null;
        SocketChannel channel = null;
        ProcessOutputCapture outputCapture = null;
        try {
            process = new ProcessBuilder(
                    config.pythonExecutable(),
                    "-m",
                    "python_runtime.worker_runtime",
                    "--socket-path",
                    socketPath.toString(),
                    "--max-frame-bytes",
                    Integer.toString(config.maxFrameBytes()),
                    "--max-in-flight-per-worker",
                    Integer.toString(config.maxInFlightPerWorker()))
                    .directory(applicationDirectory.toFile())
                    .redirectErrorStream(true)
                    .start();
            outputCapture = new ProcessOutputCapture(
                    process.getInputStream(), workerId, generation, process.pid());
            outputCapture.start();

            channel = connect(socketPath, process, startupDeadlineNs);
            PythonRuntimeProtocol.Frame ready;
            try {
                ready = readStartupFrame(channel, config.maxFrameBytes(), startupDeadlineNs);
            } catch (TimeoutException timeout) {
                throw new StartupException(ManagedPythonFailureCategory.STARTUP_TIMEOUT,
                        "Python worker readiness timed out", timeout);
            } catch (IOException malformed) {
                throw new StartupException(ManagedPythonFailureCategory.READINESS_MALFORMED,
                        "Python worker readiness frame was malformed", malformed);
            }
            if (!PythonRuntimeProtocol.TYPE_READY.equals(ready.type())) {
                throw new StartupException(ManagedPythonFailureCategory.READINESS_MALFORMED,
                        "Expected ready message, received: " + ready.type(), null);
            }
            Object reportedPid = ready.metadata().get("workerPid");
            if (!(reportedPid instanceof Number number) || number.longValue() != process.pid()) {
                throw new StartupException(ManagedPythonFailureCategory.READINESS_MALFORMED,
                        "Python worker readiness PID did not match the launched process", null);
            }
            Object reportedCapacity = ready.metadata().get("maxInFlightPerWorker");
            if (!(reportedCapacity instanceof Number capacityNumber)
                    || capacityNumber.intValue() != config.maxInFlightPerWorker()) {
                throw new StartupException(ManagedPythonFailureCategory.READINESS_CAPACITY_MISMATCH,
                        "Python worker readiness capacity did not match the configured capacity", null);
            }
            if (config.healthCheckEnabled()
                    && !Boolean.TRUE.equals(ready.metadata().get("healthCheckSupported"))) {
                throw new StartupException(ManagedPythonFailureCategory.READINESS_MALFORMED,
                        "Python worker readiness did not confirm health-check support", null);
            }
            ManagedPythonWorker worker = new ManagedPythonWorker(
                    config, workerId, generation, socketPath, process, channel, outputCapture, listener);
            LOGGER.info("event=managed_python_worker_ready workerId={} generation={} pid={} maxInFlight={}",
                    workerId, generation, process.pid(), config.maxInFlightPerWorker());
            return worker;
        } catch (Exception failure) {
            closeQuietly(channel);
            stopProcess(process, config.shutdownTimeoutMs());
            if (outputCapture != null) outputCapture.join(startupDeadlineNs);
            deleteSocket(socketPath);
            throw failure;
        }
    }

    String workerId() {
        return workerId;
    }

    int generation() {
        return generation;
    }

    long pid() {
        return workerPid;
    }

    Path socketPath() {
        return socketPath;
    }

    boolean isRunning() {
        return state == WorkerState.RUNNING;
    }

    boolean isResponsive() {
        synchronized (stateLock) {
            if (state != WorkerState.RUNNING) return false;
            if (inFlightCount > 0) return true;
            long allowedIdleNs = TimeUnit.MILLISECONDS.toNanos(
                    config.healthCheckIntervalMs() + config.healthCheckTimeoutMs());
            return !config.healthCheckEnabled()
                    || outstandingHealth != null
                    || System.nanoTime() - lastResponsiveNs <= allowedIdleNs;
        }
    }

    int inFlightCount() {
        synchronized (stateLock) {
            return inFlightCount;
        }
    }

    int availableCapacity() {
        synchronized (stateLock) {
            return state == WorkerState.RUNNING
                    ? Math.max(0, config.maxInFlightPerWorker() - inFlightCount)
                    : 0;
        }
    }

    ManagedPythonRuntimeSnapshot.Worker snapshot(boolean restarting, boolean exhausted) {
        synchronized (stateLock) {
            boolean responsive = state == WorkerState.RUNNING && (inFlightCount > 0
                    || !config.healthCheckEnabled()
                    || outstandingHealth != null
                    || System.nanoTime() - lastResponsiveNs <= TimeUnit.MILLISECONDS.toNanos(
                    config.healthCheckIntervalMs() + config.healthCheckTimeoutMs()));
            return new ManagedPythonRuntimeSnapshot.Worker(
                    workerId,
                    generation,
                    workerPid,
                    state.name(),
                    process.isAlive(),
                    responsive,
                    responsive && inFlightCount < config.maxInFlightPerWorker(),
                    inFlightCount,
                    config.maxInFlightPerWorker(),
                    outstandingHealth != null,
                    lastResponsiveEpochMillis,
                    lastHealthLatencyNanos,
                    restarting,
                    exhausted);
        }
    }

    void healthSweep(long nowNs) {
        if (!config.healthCheckEnabled()) return;
        OutstandingHealth timedOut = null;
        HealthPing ping = null;
        synchronized (stateLock) {
            if (state != WorkerState.RUNNING || inFlightCount != 0) return;
            if (outstandingHealth != null) {
                if (nowNs >= outstandingHealth.deadlineNs()) {
                    timedOut = outstandingHealth;
                    outstandingHealth = null;
                }
            } else if (nowNs - startedAtNs >= TimeUnit.MILLISECONDS.toNanos(
                    config.healthCheckStartupGraceMs())
                    && nowNs - lastResponsiveNs >= TimeUnit.MILLISECONDS.toNanos(
                    config.healthCheckIntervalMs())) {
                String id = workerId + "-g" + generation + "-h" + healthSequence.incrementAndGet();
                outstandingHealth = new OutstandingHealth(
                        id, nowNs, nowNs + TimeUnit.MILLISECONDS.toNanos(config.healthCheckTimeoutMs()));
                ping = new HealthPing(id, generation, workerPid);
            }
        }
        if (timedOut != null) {
            TimeoutException failure = new TimeoutException(
                    "Managed Python worker health check timed out: workerId=" + workerId
                            + ", generation=" + generation + ", pid=" + workerPid
                            + ", healthCheckId=" + timedOut.healthCheckId());
            markUnhealthy(ManagedPythonFailureCategory.HEALTH_CHECK_TIMEOUT, failure);
            return;
        }
        if (ping != null) {
            if (!outboundQueue.offer(ping)) {
                synchronized (stateLock) {
                    if (outstandingHealth != null
                            && outstandingHealth.healthCheckId().equals(ping.healthCheckId())) {
                        outstandingHealth = null;
                    }
                }
                markUnhealthy(ManagedPythonFailureCategory.HEALTH_CHECK_PROTOCOL_FAILURE,
                        new IOException("Managed Python worker health queue rejected control capacity"));
            }
        }
    }

    boolean tryMarkAvailabilityPublished() {
        synchronized (stateLock) {
            if (state != WorkerState.RUNNING
                    || inFlightCount >= config.maxInFlightPerWorker()
                    || availabilityPublished) {
                return false;
            }
            availabilityPublished = true;
            return true;
        }
    }

    void consumeAvailabilityPublication() {
        synchronized (stateLock) {
            availabilityPublished = false;
        }
    }

    void clearAvailabilityPublication() {
        synchronized (stateLock) {
            availabilityPublished = false;
        }
    }

    boolean hasAvailableCapacity() {
        synchronized (stateLock) {
            return state == WorkerState.RUNNING && inFlightCount < config.maxInFlightPerWorker();
        }
    }

    boolean tryReserveAndEnqueue(Assignment assignment) {
        IOException invariantFailure = null;
        int reservedCount = 0;
        synchronized (stateLock) {
            if (state != WorkerState.RUNNING
                    || assignment.completion().isDone()
                    || inFlightCount >= config.maxInFlightPerWorker()
                    || activeAssignments.containsKey(assignment.request().requestId())) {
                return false;
            }
            assignment.requestDeadlineNs = System.nanoTime()
                    + TimeUnit.MILLISECONDS.toNanos(config.requestTimeoutMs());
            assignment.assignedAtNs = System.nanoTime();
            activeAssignments.put(assignment.request().requestId(), assignment);
            inFlightCount++;
            reservedCount = inFlightCount;
            if (!outboundQueue.offer(assignment)) {
                activeAssignments.remove(assignment.request().requestId(), assignment);
                inFlightCount--;
                assignment.state = AssignmentState.TERMINAL;
                invariantFailure = new IOException(
                        "Managed Python worker outbound queue rejected reserved capacity");
            }
        }
        if (invariantFailure != null) {
            assignment.completion().completeExceptionally(invariantFailure);
            markUnhealthy(ManagedPythonFailureCategory.CAPACITY_MISMATCH, invariantFailure);
            return false;
        }
        LOGGER.debug("event=managed_python_request_assigned workerId={} generation={} pid={} requestId={} inFlight={} maxInFlight={} timeNs={}",
                workerId, generation, workerPid, assignment.request().requestId(), reservedCount,
                config.maxInFlightPerWorker(), System.nanoTime());
        return true;
    }

    void beginDrain(long deadlineNs) {
        synchronized (stateLock) {
            if (state == WorkerState.STOPPED || state == WorkerState.UNHEALTHY) return;
            shutdownDeadlineNs = deadlineNs;
            state = WorkerState.DRAINING;
            availabilityPublished = false;
            outstandingHealth = null;
        }
        outboundQueue.removeIf(item -> item instanceof HealthPing);
        writerThread.interrupt();
    }

    void requestTimedOut(Assignment assignment, TimeoutException timeout) {
        boolean preTransmission = false;
        boolean poisonWorker = false;
        synchronized (stateLock) {
            Assignment current = activeAssignments.get(assignment.request().requestId());
            if (current != assignment) return;
            if (assignment.state == AssignmentState.QUEUED) {
                activeAssignments.remove(assignment.request().requestId(), assignment);
                inFlightCount--;
                assignment.state = AssignmentState.TERMINAL;
                preTransmission = true;
            } else if (assignment.state == AssignmentState.TRANSMITTING
                    || assignment.state == AssignmentState.TRANSMITTED) {
                poisonWorker = true;
            }
        }
        if (preTransmission) {
            outboundQueue.remove(assignment);
            assignment.completion().completeExceptionally(timeout);
            listener.onRequestTerminal(this, assignment, false,
                    ManagedPythonFailureCategory.REQUEST_TIMEOUT_PRE_TRANSMISSION);
            publishCapacityIfEligible();
        } else if (poisonWorker) {
            markUnhealthy(ManagedPythonFailureCategory.REQUEST_TIMEOUT_POST_TRANSMISSION, timeout);
        }
    }

    void fail(Exception failure) {
        markUnhealthy(ManagedPythonFailureCategory.PROTOCOL_CORRUPTION, failure);
    }

    void forceStop(Exception failure) {
        List<Assignment> assignments;
        synchronized (stateLock) {
            if (state != WorkerState.STOPPED) state = WorkerState.STOPPED;
            availabilityPublished = false;
            assignments = drainAssignmentsLocked();
        }
        outboundQueue.clear();
        completeAllExceptionally(assignments, failure, ManagedPythonFailureCategory.SHUTDOWN_TIMEOUT);
        writerThread.interrupt();
        cleanup(true, System.nanoTime());
    }

    boolean isStopped() {
        return state == WorkerState.STOPPED;
    }

    void processExited() {
        WorkerState observed = state;
        if (observed != WorkerState.DRAINING && observed != WorkerState.STOPPED
                && observed != WorkerState.UNHEALTHY) {
            markUnhealthy(ManagedPythonFailureCategory.PROCESS_EXIT, new IOException(
                    "Managed Python worker exited unexpectedly: workerId=" + workerId
                            + ", generation=" + generation + ", pid=" + workerPid
                            + ", exitCode=" + process.exitValue()));
        }
        cleanup(false, System.nanoTime());
    }

    void awaitProcessExit() throws InterruptedException {
        while (process.isAlive()) {
            if (state == WorkerState.UNHEALTHY) {
                terminateUnhealthyProcess();
                return;
            }
            process.waitFor(100, TimeUnit.MILLISECONDS);
        }
    }

    boolean awaitStopped(long deadlineNs) {
        try {
            long remaining = remainingNanos(deadlineNs);
            boolean completed = remaining > 0 && stopped.await(remaining, TimeUnit.NANOSECONDS);
            if (completed) {
                joinThread(writerThread, deadlineNs);
                joinThread(responseReader, deadlineNs);
                outputCapture.join(deadlineNs);
            }
            return completed;
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private void writerLoop() {
        try {
            while (true) {
                expireAssignments();
                WorkerState observed = state;
                if (observed == WorkerState.UNHEALTHY || observed == WorkerState.STOPPED) return;
                if (observed == WorkerState.DRAINING && activeCount() == 0) {
                    gracefulStop();
                    return;
                }
                OutboundItem item;
                try {
                    item = outboundQueue.poll(WRITER_POLL_MILLIS, TimeUnit.MILLISECONDS);
                } catch (InterruptedException interrupted) {
                    if (state == WorkerState.DRAINING) continue;
                    if (state == WorkerState.UNHEALTHY || state == WorkerState.STOPPED) return;
                    Thread.currentThread().interrupt();
                    markUnhealthy(ManagedPythonFailureCategory.SOCKET_WRITE_FAILURE,
                            new IOException("Managed Python worker writer was interrupted", interrupted));
                    return;
                }
                if (item == null) continue;
                if (item instanceof Assignment assignment) transmit(assignment);
                else if (item instanceof HealthPing ping) transmitHealth(ping);
            }
        } catch (Exception failure) {
            markUnhealthy(ManagedPythonFailureCategory.SOCKET_WRITE_FAILURE,
                    asException("Managed Python worker request transmission failed", failure));
        }
    }

    private void transmit(Assignment assignment) {
        boolean expired = false;
        synchronized (stateLock) {
            Assignment current = activeAssignments.get(assignment.request().requestId());
            if (current != assignment || assignment.state != AssignmentState.QUEUED) return;
            if (System.nanoTime() >= assignment.requestDeadlineNs) {
                expired = true;
            } else {
                assignment.state = AssignmentState.TRANSMITTING;
            }
        }
        if (expired) {
            requestTimedOut(assignment, requestTimeout());
            return;
        }
        try {
            writeRequest(assignment.request());
            synchronized (stateLock) {
                Assignment current = activeAssignments.get(assignment.request().requestId());
                if (current == assignment && assignment.state == AssignmentState.TRANSMITTING) {
                    assignment.state = AssignmentState.TRANSMITTED;
                }
            }
            LOGGER.debug("event=managed_python_request_transmitted workerId={} generation={} pid={} requestId={} timeNs={}",
                    workerId, generation, workerPid, assignment.request().requestId(), System.nanoTime());
        } catch (Exception failure) {
            markUnhealthy(ManagedPythonFailureCategory.SOCKET_WRITE_FAILURE,
                    asException("Managed Python worker transport failed", failure));
        }
    }

    private void transmitHealth(HealthPing ping) {
        synchronized (stateLock) {
            if (state != WorkerState.RUNNING || outstandingHealth == null
                    || !outstandingHealth.healthCheckId().equals(ping.healthCheckId())) return;
        }
        try {
            PythonRuntimeProtocol.writeFrame(channel, Map.of(
                    "protocolVersion", PythonRuntimeProtocol.VERSION,
                    "type", PythonRuntimeProtocol.TYPE_PING,
                    "healthCheckId", ping.healthCheckId(),
                    "workerGeneration", ping.generation(),
                    "expectedWorkerPid", ping.workerPid()), new byte[0], config.maxFrameBytes());
            listener.onHealthCheckSent(this, ping.healthCheckId());
            LOGGER.debug("event=managed_python_health_ping workerId={} generation={} pid={} healthCheckId={} timeNs={}",
                    workerId, generation, workerPid, ping.healthCheckId(), System.nanoTime());
        } catch (Exception failure) {
            markUnhealthy(ManagedPythonFailureCategory.SOCKET_WRITE_FAILURE,
                    asException("Managed Python worker health transmission failed", failure));
        }
    }

    private void expireAssignments() {
        List<Assignment> queuedExpired = new ArrayList<>();
        Assignment ambiguous = null;
        long now = System.nanoTime();
        synchronized (stateLock) {
            for (Assignment assignment : new ArrayList<>(activeAssignments.values())) {
                if (now < assignment.requestDeadlineNs) continue;
                if (assignment.state == AssignmentState.QUEUED) {
                    if (activeAssignments.remove(assignment.request().requestId(), assignment)) {
                        inFlightCount--;
                        assignment.state = AssignmentState.TERMINAL;
                        queuedExpired.add(assignment);
                    }
                } else if (assignment.state == AssignmentState.TRANSMITTING
                        || assignment.state == AssignmentState.TRANSMITTED) {
                    ambiguous = assignment;
                    break;
                }
            }
        }
        for (Assignment assignment : queuedExpired) {
            outboundQueue.remove(assignment);
            assignment.completion().completeExceptionally(requestTimeout());
            listener.onRequestTerminal(this, assignment, false,
                    ManagedPythonFailureCategory.REQUEST_TIMEOUT_PRE_TRANSMISSION);
        }
        if (!queuedExpired.isEmpty()) publishCapacityIfEligible();
        if (ambiguous != null) markUnhealthy(
                ManagedPythonFailureCategory.REQUEST_TIMEOUT_POST_TRANSMISSION, requestTimeout());
    }

    private void responseLoop() {
        try {
            while (true) {
                PythonRuntimeProtocol.Frame frame = PythonRuntimeProtocol.readFrame(channel, config.maxFrameBytes());
                if (PythonRuntimeProtocol.TYPE_SHUTDOWN_ACK.equals(frame.type())) {
                    shutdownAcknowledged.countDown();
                    return;
                }
                if (PythonRuntimeProtocol.TYPE_PONG.equals(frame.type())) {
                    handlePong(frame.metadata());
                    continue;
                }
                String requestId = stringValue(frame.metadata(), "requestId");
                Assignment assignment;
                synchronized (stateLock) {
                    assignment = activeAssignments.get(requestId);
                }
                if (assignment == null) {
                    if (state == WorkerState.RUNNING) {
                        markUnhealthy(ManagedPythonFailureCategory.UNKNOWN_RESPONSE_ID,
                                new IOException("Unknown or duplicate response requestId: " + requestId));
                        return;
                    }
                    continue;
                }
                if (System.nanoTime() >= assignment.requestDeadlineNs) {
                    requestTimedOut(assignment, requestTimeout());
                    continue;
                }
                if (PythonRuntimeProtocol.TYPE_RESPONSE.equals(frame.type())) {
                    int status = numberValue(frame.metadata(), "status");
                    finishAssignment(assignment, new PythonCallResponse(
                            status, responseHeaders(frame.metadata().get("headers")), frame.body()), null);
                } else if (PythonRuntimeProtocol.TYPE_ERROR.equals(frame.type())) {
                    String errorCode = stringValue(frame.metadata(), "code");
                    String errorMessage = stringValue(frame.metadata(), "message");
                    IOException error = pythonError(requestId, errorCode, errorMessage);
                    if (ASGI_EXECUTION_FAILED.equals(errorCode)) {
                        finishAssignment(assignment, null, error);
                    } else if (CAPACITY_EXCEEDED.equals(errorCode)) {
                        markUnhealthy(ManagedPythonFailureCategory.CAPACITY_MISMATCH, error);
                    } else {
                        markUnhealthy(ManagedPythonFailureCategory.PROTOCOL_CORRUPTION, new IOException(
                                "Unrecognized Python worker error code; capacity and correlation integrity "
                                        + "cannot be trusted: " + error.getMessage(), error));
                    }
                } else {
                    throw new IOException("Unexpected Python runtime message: " + frame.type());
                }
            }
        } catch (Exception failure) {
            if (state != WorkerState.STOPPED) {
                ManagedPythonFailureCategory category = failure instanceof EOFException
                        ? ManagedPythonFailureCategory.SOCKET_EOF
                        : ManagedPythonFailureCategory.SOCKET_READ_FAILURE;
                markUnhealthy(category, asException("Managed Python worker connection failed", failure));
            }
        }
    }

    private void handlePong(Map<String, Object> metadata) {
        String healthCheckId;
        int reportedGeneration;
        long reportedPid;
        try {
            healthCheckId = stringValue(metadata, "healthCheckId");
            reportedGeneration = numberValue(metadata, "workerGeneration");
            reportedPid = longValue(metadata, "workerPid");
            numberValue(metadata, "activeTaskCount");
        } catch (IOException failure) {
            markUnhealthy(ManagedPythonFailureCategory.HEALTH_CHECK_PROTOCOL_FAILURE, failure);
            return;
        }
        long latency;
        boolean invalidCorrelation = false;
        synchronized (stateLock) {
            if (state != WorkerState.RUNNING) return;
            if (reportedGeneration != generation) return;
            if (reportedPid != workerPid || outstandingHealth == null
                    || !outstandingHealth.healthCheckId().equals(healthCheckId)) {
                invalidCorrelation = true;
                latency = 0;
            } else {
                latency = Math.max(0, System.nanoTime() - outstandingHealth.startedAtNs());
                outstandingHealth = null;
                lastHealthLatencyNanos = latency;
                updateResponsiveLocked();
            }
        }
        if (invalidCorrelation) {
            markUnhealthy(ManagedPythonFailureCategory.HEALTH_CHECK_PROTOCOL_FAILURE,
                    new IOException("Health response correlation did not match the current worker generation"));
            return;
        }
        listener.onHealthCheckSucceeded(this, healthCheckId, latency);
        LOGGER.debug("event=managed_python_health_pong workerId={} generation={} pid={} healthCheckId={} latencyNs={}",
                workerId, generation, workerPid, healthCheckId, latency);
    }

    private void finishAssignment(
            Assignment assignment, PythonCallResponse response, Exception failure) {
        int remainingCount;
        synchronized (stateLock) {
            if (!activeAssignments.remove(assignment.request().requestId(), assignment)) return;
            assignment.state = AssignmentState.TERMINAL;
            inFlightCount--;
            remainingCount = inFlightCount;
        }
        if (failure == null) assignment.completion().complete(response);
        else assignment.completion().completeExceptionally(failure);
        markResponsive();
        listener.onRequestTerminal(this, assignment, failure == null, null);
        LOGGER.debug("event=managed_python_request_completed workerId={} generation={} pid={} requestId={} inFlight={} maxInFlight={} timeNs={}",
                workerId, generation, workerPid, assignment.request().requestId(), remainingCount,
                config.maxInFlightPerWorker(), System.nanoTime());
        publishCapacityIfEligible();
    }

    private void publishCapacityIfEligible() {
        if (state == WorkerState.RUNNING && hasAvailableCapacity()) listener.onCapacityAvailable(this);
    }

    private int activeCount() {
        synchronized (stateLock) {
            return inFlightCount;
        }
    }

    private void writeRequest(PythonCallRequest request) throws IOException {
        Map<String, Object> metadata = new LinkedHashMap<>();
        metadata.put("protocolVersion", PythonRuntimeProtocol.VERSION);
        metadata.put("type", PythonRuntimeProtocol.TYPE_REQUEST);
        metadata.put("requestId", request.requestId());
        metadata.put("method", request.method());
        metadata.put("path", request.target().getRawPath());
        metadata.put("queryString", request.target().getRawQuery() == null ? "" : request.target().getRawQuery());
        metadata.put("headers", request.headers().entrySet().stream()
                .map(entry -> List.of(entry.getKey(), entry.getValue()))
                .toList());
        PythonRuntimeProtocol.writeFrame(channel, metadata, request.body(), config.maxFrameBytes());
    }

    private void gracefulStop() {
        try {
            long remaining = remainingNanos(shutdownDeadlineNs);
            if (remaining > 0 && channel.isOpen()) {
                PythonRuntimeProtocol.writeFrame(channel, Map.of(
                        "protocolVersion", PythonRuntimeProtocol.VERSION,
                        "type", PythonRuntimeProtocol.TYPE_SHUTDOWN), new byte[0], config.maxFrameBytes());
                remaining = remainingNanos(shutdownDeadlineNs);
                if (remaining > 0) shutdownAcknowledged.await(remaining, TimeUnit.NANOSECONDS);
            }
        } catch (Exception ignored) {
            // The common-deadline cleanup below is the deterministic fallback.
        } finally {
            synchronized (stateLock) {
                state = WorkerState.STOPPED;
                availabilityPublished = false;
            }
            cleanup(false, shutdownDeadlineNs);
        }
    }

    private void markUnhealthy(ManagedPythonFailureCategory category, Exception failure) {
        boolean publish;
        List<Assignment> assignments;
        synchronized (stateLock) {
            if (state == WorkerState.UNHEALTHY || state == WorkerState.STOPPED) return;
            state = WorkerState.UNHEALTHY;
            availabilityPublished = false;
            outstandingHealth = null;
            assignments = drainAssignmentsLocked();
            publish = failurePublished.compareAndSet(false, true);
        }
        outboundQueue.clear();
        completeAllExceptionally(assignments, failure, category);
        writerThread.interrupt();
        closeQuietly(channel);
        if (process.isAlive()) process.destroy();
        if (publish) {
            LOGGER.warn("event=managed_python_worker_unhealthy workerId={} generation={} pid={} "
                            + "failureCategory={} failure={}",
                    workerId, generation, workerPid, category, boundedMessage(failure));
            listener.onUnhealthy(this, category, failure);
        }
    }

    private List<Assignment> drainAssignmentsLocked() {
        List<Assignment> assignments = new ArrayList<>(activeAssignments.values());
        activeAssignments.clear();
        inFlightCount = 0;
        assignments.forEach(assignment -> assignment.state = AssignmentState.TERMINAL);
        return assignments;
    }

    private void completeAllExceptionally(
            List<Assignment> assignments,
            Exception failure,
            ManagedPythonFailureCategory category) {
        assignments.forEach(assignment -> {
            assignment.completion().completeExceptionally(failure);
            listener.onRequestTerminal(this, assignment, false, category);
        });
    }

    private void terminateUnhealthyProcess() {
        long deadlineNs = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.shutdownTimeoutMs());
        if (!cleanupStarted.compareAndSet(false, true)) {
            awaitStopped(deadlineNs);
            return;
        }

        boolean interrupted = false;
        closeQuietly(channel);
        try {
            if (process.isAlive()) {
                process.destroy();
                long gracefulWaitNs = remainingNanos(deadlineNs) / 2;
                if (gracefulWaitNs > 0) process.waitFor(gracefulWaitNs, TimeUnit.NANOSECONDS);
            }
            if (process.isAlive()) {
                LOGGER.warn("event=managed_python_worker_force_terminated workerId={} generation={} pid={}",
                        workerId, generation, workerPid);
                listener.onForcedTermination(this);
                process.destroyForcibly();
                long remaining = remainingNanos(deadlineNs);
                if (remaining > 0) process.waitFor(remaining, TimeUnit.NANOSECONDS);
            }
        } catch (InterruptedException exception) {
            interrupted = true;
            if (process.isAlive()) process.destroyForcibly();
            try {
                long remaining = remainingNanos(deadlineNs);
                if (remaining > 0) process.waitFor(remaining, TimeUnit.NANOSECONDS);
            } catch (InterruptedException repeated) {
                // Cleanup below still unblocks the slot; interrupt status is restored afterward.
            }
        } finally {
            finishCleanup();
            if (interrupted) Thread.currentThread().interrupt();
        }
    }

    private void cleanup(boolean force, long deadlineNs) {
        if (!cleanupStarted.compareAndSet(false, true)) return;
        closeQuietly(channel);
        try {
            if (process.isAlive()) {
                if (force) process.destroyForcibly();
                else process.destroy();
                long remaining = remainingNanos(deadlineNs);
                if (remaining > 0) process.waitFor(remaining, TimeUnit.NANOSECONDS);
                if (process.isAlive()) {
                    listener.onForcedTermination(this);
                    process.destroyForcibly();
                }
            }
        } catch (InterruptedException exception) {
            process.destroyForcibly();
            Thread.currentThread().interrupt();
        } finally {
            finishCleanup();
        }
    }

    private void finishCleanup() {
        deleteSocket(socketPath);
        outputCapture.join(System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(250));
        synchronized (stateLock) {
            state = WorkerState.STOPPED;
            availabilityPublished = false;
        }
        stopped.countDown();
        if (Files.exists(socketPath)) {
            listener.onCleanupFailure(this, "Worker socket remained after cleanup", null);
        }
        if (process.isAlive()) {
            listener.onCleanupFailure(this,
                    "Worker process remained alive after forced cleanup: pid=" + workerPid, null);
        }
        LOGGER.info("event=managed_python_worker_stopped workerId={} generation={} pid={} socketRemoved={}",
                workerId, generation, workerPid, !Files.exists(socketPath));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, List<String>> responseHeaders(Object value) throws IOException {
        if (!(value instanceof List<?> entries)) throw new IOException("Response headers are invalid");
        Map<String, List<String>> result = new LinkedHashMap<>();
        for (Object entry : entries) {
            if (!(entry instanceof List<?> pair) || pair.size() != 2) {
                throw new IOException("Response header is invalid");
            }
            result.computeIfAbsent(String.valueOf(pair.get(0)), ignored -> new ArrayList<>())
                    .add(String.valueOf(pair.get(1)));
        }
        return result;
    }

    private static SocketChannel connect(Path socketPath, Process process, long deadlineNs) throws Exception {
        Exception lastFailure = null;
        while (System.nanoTime() < deadlineNs) {
            if (!process.isAlive()) {
                throw new IOException("Python worker exited during startup with code " + process.exitValue());
            }
            if (Files.exists(socketPath)) {
                SocketChannel candidate = SocketChannel.open(StandardProtocolFamily.UNIX);
                try {
                    candidate.connect(UnixDomainSocketAddress.of(socketPath));
                    return candidate;
                } catch (Exception failure) {
                    lastFailure = failure;
                    closeQuietly(candidate);
                }
            }
            Thread.sleep(25);
        }
        throw new TimeoutException("Timed out connecting to Python runtime socket"
                + (lastFailure == null ? "" : ": " + lastFailure.getMessage()));
    }

    private static PythonRuntimeProtocol.Frame readStartupFrame(
            SocketChannel channel, int maxFrameBytes, long deadlineNs) throws Exception {
        FutureTask<PythonRuntimeProtocol.Frame> task = new FutureTask<>(
                () -> PythonRuntimeProtocol.readFrame(channel, maxFrameBytes));
        Thread thread = new Thread(task, "managed-python-readiness");
        thread.start();
        long remaining = remainingNanos(deadlineNs);
        if (remaining <= 0) throw new TimeoutException("Python worker readiness timed out");
        try {
            return task.get(remaining, TimeUnit.NANOSECONDS);
        } catch (ExecutionException failure) {
            Throwable cause = failure.getCause();
            if (cause instanceof IOException ioFailure) throw ioFailure;
            if (cause instanceof Exception exception) throw exception;
            throw new IOException("Python worker readiness failed", cause);
        } catch (TimeoutException timeout) {
            closeQuietly(channel);
            throw new TimeoutException("Python worker readiness timed out");
        } finally {
            if (!task.isDone()) task.cancel(true);
            joinThread(thread, deadlineNs);
        }
    }

    private static String stringValue(Map<String, Object> metadata, String name) throws IOException {
        Object value = metadata.get(name);
        if (!(value instanceof String text) || text.isBlank()) {
            throw new IOException("Missing protocol field: " + name);
        }
        return text;
    }

    private static int numberValue(Map<String, Object> metadata, String name) throws IOException {
        Object value = metadata.get(name);
        if (!(value instanceof Number number)) throw new IOException("Missing numeric protocol field: " + name);
        return number.intValue();
    }

    private static long longValue(Map<String, Object> metadata, String name) throws IOException {
        Object value = metadata.get(name);
        if (!(value instanceof Number number)) throw new IOException("Missing numeric protocol field: " + name);
        return number.longValue();
    }

    private void markResponsive() {
        synchronized (stateLock) {
            updateResponsiveLocked();
        }
    }

    private void updateResponsiveLocked() {
        lastResponsiveNs = System.nanoTime();
        lastResponsiveEpochMillis = System.currentTimeMillis();
    }

    private TimeoutException requestTimeout() {
        return new TimeoutException("Managed Python request timed out: workerId=" + workerId
                + ", generation=" + generation + ", pid=" + workerPid);
    }

    private IOException pythonError(String requestId, String errorCode, String errorMessage) {
        return new IOException("Managed Python Runtime error: workerId=" + workerId
                + ", generation=" + generation
                + ", pid=" + workerPid
                + ", requestId=" + requestId
                + ", code=" + errorCode
                + ", message=" + errorMessage);
    }

    private static Exception asException(String message, Throwable failure) {
        return failure instanceof Exception exception
                ? new IOException(message, exception)
                : new IOException(message, failure);
    }

    private static void stopProcess(Process process, long timeoutMs) {
        if (process == null) return;
        try {
            if (process.isAlive() && !process.waitFor(timeoutMs, TimeUnit.MILLISECONDS)) {
                process.destroy();
                if (!process.waitFor(timeoutMs, TimeUnit.MILLISECONDS)) process.destroyForcibly();
            }
        } catch (InterruptedException exception) {
            process.destroyForcibly();
            Thread.currentThread().interrupt();
        }
    }

    private static void deleteSocket(Path socketPath) {
        try {
            Files.deleteIfExists(socketPath);
        } catch (IOException failure) {
            LOGGER.error("event=managed_python_cleanup_failed resource=worker_socket path={} failure={}",
                    socketPath, boundedMessage(failure));
        }
    }

    private static void closeQuietly(AutoCloseable closeable) {
        if (closeable == null) return;
        try {
            closeable.close();
        } catch (Exception ignored) {
            // Best effort after the primary failure.
        }
    }

    private static long remainingNanos(long deadlineNs) {
        return Math.max(0, deadlineNs - System.nanoTime());
    }

    private static void joinThread(Thread thread, long deadlineNs) {
        if (thread == null || thread == Thread.currentThread()) return;
        try {
            long remaining = remainingNanos(deadlineNs);
            if (remaining > 0) TimeUnit.NANOSECONDS.timedJoin(thread, remaining);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
    }

    private static String boundedMessage(Throwable failure) {
        if (failure == null) return "none";
        String message = failure.getMessage();
        if (message == null || message.isBlank()) message = failure.getClass().getSimpleName();
        message = message.replace('\n', ' ').replace('\r', ' ');
        return message.length() <= 512 ? message : message.substring(0, 512);
    }

    private static final class ProcessOutputCapture {
        private static final int READ_BUFFER_BYTES = 4_096;
        private static final int LOG_CHUNK_LIMIT = 1_024;
        private static final int MAX_CHUNKS_PER_MINUTE = 20;

        private final InputStream input;
        private final String workerId;
        private final int generation;
        private final long pid;
        private final Thread thread;

        private ProcessOutputCapture(InputStream input, String workerId, int generation, long pid) {
            this.input = input;
            this.workerId = workerId;
            this.generation = generation;
            this.pid = pid;
            this.thread = new Thread(this::drain,
                    "managed-python-output-" + workerId + "-g" + generation);
        }

        private void start() {
            thread.start();
        }

        private void drain() {
            byte[] buffer = new byte[READ_BUFFER_BYTES];
            long windowStarted = System.nanoTime();
            int emitted = 0;
            long suppressed = 0;
            try (input) {
                int count;
                while ((count = input.read(buffer)) >= 0) {
                    long now = System.nanoTime();
                    if (now - windowStarted >= TimeUnit.MINUTES.toNanos(1)) {
                        if (suppressed > 0) logSuppressed(suppressed);
                        windowStarted = now;
                        emitted = 0;
                        suppressed = 0;
                    }
                    if (emitted < MAX_CHUNKS_PER_MINUTE) {
                        String output = new String(buffer, 0, Math.min(count, LOG_CHUNK_LIMIT),
                                java.nio.charset.StandardCharsets.UTF_8)
                                .replace('\n', ' ').replace('\r', ' ');
                        LOGGER.debug("event=managed_python_worker_output workerId={} generation={} pid={} "
                                        + "stream=combined output={}",
                                workerId, generation, pid, output);
                        emitted++;
                    } else {
                        suppressed++;
                    }
                }
            } catch (IOException failure) {
                LOGGER.debug("event=managed_python_worker_output_closed workerId={} generation={} pid={} failure={}",
                        workerId, generation, pid, boundedMessage(failure));
            } finally {
                if (suppressed > 0) logSuppressed(suppressed);
            }
        }

        private void logSuppressed(long suppressed) {
            LOGGER.debug("event=managed_python_worker_output_suppressed workerId={} generation={} pid={} chunks={}",
                    workerId, generation, pid, suppressed);
        }

        private void join(long deadlineNs) {
            joinThread(thread, deadlineNs);
        }
    }
}
