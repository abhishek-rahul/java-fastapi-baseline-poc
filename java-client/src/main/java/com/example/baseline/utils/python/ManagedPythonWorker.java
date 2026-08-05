package com.example.baseline.utils.python;

import com.example.baseline.utils.config.ApplicationConfig.ManagedPythonRuntimeConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

final class ManagedPythonWorker {
    private static final Logger LOGGER = LoggerFactory.getLogger(ManagedPythonWorker.class);
    private static final long WRITER_POLL_MILLIS = 25;

    interface Listener {
        void onCapacityAvailable(ManagedPythonWorker worker);

        void onUnhealthy(ManagedPythonWorker worker, Exception failure);
    }

    static final class Assignment {
        private final PythonCallRequest request;
        private final CompletableFuture<PythonCallResponse> completion;
        private long requestDeadlineNs;
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
    private final Listener listener;
    private final Object stateLock = new Object();
    private final ArrayBlockingQueue<Assignment> outboundQueue;
    private final Map<String, Assignment> activeAssignments = new HashMap<>();
    private final AtomicBoolean failurePublished = new AtomicBoolean();
    private final AtomicBoolean cleanupStarted = new AtomicBoolean();
    private final CountDownLatch shutdownAcknowledged = new CountDownLatch(1);
    private final CountDownLatch stopped = new CountDownLatch(1);
    private final Thread writerThread;
    private final Thread responseReader;
    private volatile WorkerState state = WorkerState.RUNNING;
    private volatile long shutdownDeadlineNs = Long.MAX_VALUE;
    private int inFlightCount;
    private boolean availabilityPublished;

    private ManagedPythonWorker(
            ManagedPythonRuntimeConfig config,
            String workerId,
            int generation,
            Path socketPath,
            Process process,
            SocketChannel channel,
            Listener listener) {
        this.config = config;
        this.workerId = workerId;
        this.generation = generation;
        this.socketPath = socketPath;
        this.process = process;
        this.workerPid = process.pid();
        this.channel = channel;
        this.listener = listener;
        this.outboundQueue = new ArrayBlockingQueue<>(config.maxInFlightPerWorker(), true);
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
            Listener listener) throws Exception {
        Path applicationDirectory = Path.of(config.applicationDirectory());
        Path socketPath = runtimeDirectory.resolve(workerId + "-g" + generation + ".sock");
        Process process = null;
        SocketChannel channel = null;
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
                    .inheritIO()
                    .start();

            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.startupTimeoutMs());
            channel = connect(socketPath, process, deadline);
            PythonRuntimeProtocol.Frame ready = readStartupFrame(channel, config.maxFrameBytes(), deadline);
            if (!"ready".equals(ready.type())) {
                throw new IOException("Expected ready message, received: " + ready.type());
            }
            Object reportedPid = ready.metadata().get("workerPid");
            if (!(reportedPid instanceof Number number) || number.longValue() != process.pid()) {
                throw new IOException("Python worker readiness PID did not match the launched process");
            }
            Object reportedCapacity = ready.metadata().get("maxInFlightPerWorker");
            if (!(reportedCapacity instanceof Number capacityNumber)
                    || capacityNumber.intValue() != config.maxInFlightPerWorker()) {
                throw new IOException("Python worker readiness capacity did not match the configured capacity");
            }
            ManagedPythonWorker worker = new ManagedPythonWorker(
                    config, workerId, generation, socketPath, process, channel, listener);
            LOGGER.debug("managedPythonEvent=ready workerId={} generation={} pid={} maxInFlight={}",
                    workerId, generation, process.pid(), config.maxInFlightPerWorker());
            return worker;
        } catch (Exception failure) {
            closeQuietly(channel);
            stopProcess(process, config.shutdownTimeoutMs());
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
            markUnhealthy(invariantFailure);
            return false;
        }
        LOGGER.debug("managedPythonEvent=assigned workerId={} generation={} pid={} requestId={} inFlight={} maxInFlight={} timeNs={}",
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
        }
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
            publishCapacityIfEligible();
        } else if (poisonWorker) {
            markUnhealthy(timeout);
        }
    }

    void fail(Exception failure) {
        markUnhealthy(failure);
    }

    void forceStop(Exception failure) {
        List<Assignment> assignments;
        synchronized (stateLock) {
            if (state != WorkerState.STOPPED) state = WorkerState.STOPPED;
            availabilityPublished = false;
            assignments = drainAssignmentsLocked();
        }
        outboundQueue.clear();
        completeAllExceptionally(assignments, failure);
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
            markUnhealthy(new IOException(
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
            return remaining > 0 && stopped.await(remaining, TimeUnit.NANOSECONDS);
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
                Assignment assignment;
                try {
                    assignment = outboundQueue.poll(WRITER_POLL_MILLIS, TimeUnit.MILLISECONDS);
                } catch (InterruptedException interrupted) {
                    if (state == WorkerState.DRAINING) continue;
                    if (state == WorkerState.UNHEALTHY || state == WorkerState.STOPPED) return;
                    Thread.currentThread().interrupt();
                    markUnhealthy(new IOException("Managed Python worker writer was interrupted", interrupted));
                    return;
                }
                if (assignment == null) continue;
                transmit(assignment);
            }
        } catch (Exception failure) {
            markUnhealthy(asException("Managed Python worker request transmission failed", failure));
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
            LOGGER.debug("managedPythonEvent=transmitted workerId={} generation={} pid={} requestId={} timeNs={}",
                    workerId, generation, workerPid, assignment.request().requestId(), System.nanoTime());
        } catch (Exception failure) {
            markUnhealthy(asException("Managed Python worker transport failed", failure));
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
        }
        if (!queuedExpired.isEmpty()) publishCapacityIfEligible();
        if (ambiguous != null) markUnhealthy(requestTimeout());
    }

    private void responseLoop() {
        try {
            while (true) {
                PythonRuntimeProtocol.Frame frame = PythonRuntimeProtocol.readFrame(channel, config.maxFrameBytes());
                if ("shutdown_ack".equals(frame.type())) {
                    shutdownAcknowledged.countDown();
                    return;
                }
                String requestId = stringValue(frame.metadata(), "requestId");
                Assignment assignment;
                synchronized (stateLock) {
                    assignment = activeAssignments.get(requestId);
                }
                if (assignment == null) {
                    if (state == WorkerState.RUNNING) {
                        throw new IOException("Unknown or duplicate response requestId: " + requestId);
                    }
                    continue;
                }
                if (System.nanoTime() >= assignment.requestDeadlineNs) {
                    requestTimedOut(assignment, requestTimeout());
                    continue;
                }
                if ("response".equals(frame.type())) {
                    int status = numberValue(frame.metadata(), "status");
                    finishAssignment(assignment, new PythonCallResponse(
                            status, responseHeaders(frame.metadata().get("headers")), frame.body()), null);
                } else if ("error".equals(frame.type())) {
                    finishAssignment(assignment, null, new IOException(
                            "Managed Python Runtime error: "
                                    + frame.metadata().getOrDefault("message", "unknown error")));
                } else {
                    throw new IOException("Unexpected Python runtime message: " + frame.type());
                }
            }
        } catch (Exception failure) {
            if (state != WorkerState.STOPPED) {
                markUnhealthy(asException("Managed Python worker connection failed", failure));
            }
        }
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
        LOGGER.debug("managedPythonEvent=completed workerId={} generation={} pid={} requestId={} inFlight={} maxInFlight={} timeNs={}",
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
        metadata.put("type", "request");
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
                        "type", "shutdown"), new byte[0], config.maxFrameBytes());
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

    private void markUnhealthy(Exception failure) {
        boolean publish;
        List<Assignment> assignments;
        synchronized (stateLock) {
            if (state == WorkerState.UNHEALTHY || state == WorkerState.STOPPED) return;
            state = WorkerState.UNHEALTHY;
            availabilityPublished = false;
            assignments = drainAssignmentsLocked();
            publish = failurePublished.compareAndSet(false, true);
        }
        outboundQueue.clear();
        completeAllExceptionally(assignments, failure);
        writerThread.interrupt();
        closeQuietly(channel);
        if (process.isAlive()) process.destroy();
        if (publish) {
            System.err.printf("Managed Python worker unhealthy: workerId=%s, generation=%d, pid=%d, failure=%s%n",
                    workerId, generation, workerPid, failure.getMessage());
            listener.onUnhealthy(this, failure);
        }
    }

    private List<Assignment> drainAssignmentsLocked() {
        List<Assignment> assignments = new ArrayList<>(activeAssignments.values());
        activeAssignments.clear();
        inFlightCount = 0;
        assignments.forEach(assignment -> assignment.state = AssignmentState.TERMINAL);
        return assignments;
    }

    private static void completeAllExceptionally(List<Assignment> assignments, Exception failure) {
        assignments.forEach(assignment -> assignment.completion().completeExceptionally(failure));
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
                System.err.printf(
                        "Managed Python worker force termination: workerId=%s, generation=%d, pid=%d%n",
                        workerId, generation, workerPid);
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
                if (process.isAlive()) process.destroyForcibly();
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
        synchronized (stateLock) {
            state = WorkerState.STOPPED;
            availabilityPublished = false;
        }
        stopped.countDown();
        LOGGER.debug("managedPythonEvent=stopped workerId={} generation={} pid={} socketRemoved={}",
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
        } catch (TimeoutException timeout) {
            closeQuietly(channel);
            throw new TimeoutException("Python worker readiness timed out");
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

    private TimeoutException requestTimeout() {
        return new TimeoutException("Managed Python request timed out: workerId=" + workerId
                + ", generation=" + generation + ", pid=" + workerPid);
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
            System.err.printf("Unable to remove managed worker socket %s: %s%n", socketPath, failure.getMessage());
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
}
