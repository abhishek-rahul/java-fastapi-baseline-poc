package com.example.baseline.utils.python;

import com.example.baseline.utils.config.ApplicationConfig.ManagedPythonRuntimeConfig;

import java.io.IOException;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
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
import java.util.concurrent.atomic.AtomicReference;

final class ManagedPythonWorker {
    interface Listener {
        void onReady(ManagedPythonWorker worker);

        void onUnhealthy(ManagedPythonWorker worker, Exception failure);
    }

    record Assignment(
            PythonCallRequest request,
            CompletableFuture<PythonCallResponse> completion,
            long requestDeadlineNs) {
    }

    private enum WorkerState {
        NEW,
        STARTING,
        READY,
        BUSY,
        DRAINING,
        UNHEALTHY,
        STOPPED
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
    private final Object outputLock = new Object();
    private final ArrayBlockingQueue<Assignment> requestQueue = new ArrayBlockingQueue<>(1);
    private final AtomicReference<Assignment> activeAssignment = new AtomicReference<>();
    private final AtomicBoolean failurePublished = new AtomicBoolean();
    private final AtomicBoolean cleanupStarted = new AtomicBoolean();
    private final CountDownLatch shutdownAcknowledged = new CountDownLatch(1);
    private final CountDownLatch stopped = new CountDownLatch(1);
    private final Thread requestRunner;
    private final Thread responseReader;
    private volatile WorkerState state = WorkerState.NEW;
    private volatile long shutdownDeadlineNs = Long.MAX_VALUE;

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
        this.state = WorkerState.READY;
        this.requestRunner = new Thread(this::requestLoop,
                "managed-python-request-" + workerId + "-g" + generation);
        this.responseReader = new Thread(this::responseLoop,
                "managed-python-response-" + workerId + "-g" + generation);
        this.requestRunner.start();
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
                    Integer.toString(config.maxFrameBytes()))
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
            ManagedPythonWorker worker = new ManagedPythonWorker(
                    config, workerId, generation, socketPath, process, channel, listener);
            /*
                    System.out.printf("Managed Python worker ready: workerId=%s, generation=%d, pid=%d, socket=%s%n",
                    workerId, generation, process.pid(), socketPath);
            */
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

    boolean isReady() {
        return state == WorkerState.READY;
    }

    boolean tryAssign(Assignment assignment) {
        synchronized (stateLock) {
            if (state != WorkerState.READY) return false;
            state = WorkerState.BUSY;
            if (!activeAssignment.compareAndSet(null, assignment)) {
                state = WorkerState.UNHEALTHY;
                return false;
            }
            if (!requestQueue.offer(assignment)) {
                activeAssignment.compareAndSet(assignment, null);
                state = WorkerState.UNHEALTHY;
                return false;
            }
        }
        /* 
        System.out.printf("Managed Python request assigned: workerId=%s, generation=%d, pid=%d, requestId=%s%n",
                workerId, generation, workerPid, assignment.request().requestId());
        */
                return true;
    }

    void beginDrain(long deadlineNs) {
        boolean wakeIdleRunner = false;
        synchronized (stateLock) {
            if (state == WorkerState.STOPPED || state == WorkerState.UNHEALTHY) return;
            shutdownDeadlineNs = deadlineNs;
            if (state == WorkerState.READY) wakeIdleRunner = true;
            state = WorkerState.DRAINING;
        }
        if (wakeIdleRunner) requestRunner.interrupt();
    }

    void requestTimedOut(Assignment assignment, TimeoutException timeout) {
        if (activeAssignment.get() != assignment) return;
        assignment.completion().completeExceptionally(timeout);
        markUnhealthy(timeout);
    }

    void forceStop(Exception failure) {
        Assignment assignment = activeAssignment.getAndSet(null);
        if (assignment != null) assignment.completion().completeExceptionally(failure);
        synchronized (stateLock) {
            if (state != WorkerState.STOPPED) state = WorkerState.STOPPED;
        }
        requestRunner.interrupt();
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
        process.waitFor();
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

    private void requestLoop() {
        try {
            while (true) {
                Assignment assignment;
                try {
                    assignment = requestQueue.take();
                } catch (InterruptedException exception) {
                    if (state == WorkerState.DRAINING || state == WorkerState.STOPPED) break;
                    Thread.currentThread().interrupt();
                    markUnhealthy(new IOException("Managed Python worker request runner was interrupted", exception));
                    return;
                }
                execute(assignment);
                if (state == WorkerState.DRAINING || state == WorkerState.STOPPED
                        || state == WorkerState.UNHEALTHY) break;
            }
            if (state == WorkerState.DRAINING) gracefulStop();
        } catch (Exception failure) {
            markUnhealthy(asException("Managed Python worker request execution failed", failure));
        }
    }

    private void execute(Assignment assignment) {
        try {
            writeRequest(assignment.request());
            long remaining = remainingNanos(assignment.requestDeadlineNs());
            if (remaining <= 0) throw requestTimeout();
            assignment.completion().get(remaining, TimeUnit.NANOSECONDS);
        } catch (TimeoutException waitTimeout) {
            TimeoutException timeout = requestTimeout();
            timeout.initCause(waitTimeout);
            assignment.completion().completeExceptionally(timeout);
            markUnhealthy(timeout);
        } catch (ExecutionException requestFailure) {
            // A correlated Python error is request-specific and leaves the transport usable.
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            assignment.completion().completeExceptionally(interrupted);
            markUnhealthy(new IOException("Managed Python worker request wait was interrupted", interrupted));
        } catch (Exception transportFailure) {
            assignment.completion().completeExceptionally(transportFailure);
            markUnhealthy(asException("Managed Python worker transport failed", transportFailure));
        } finally {
            finishAssignment(assignment);
        }
    }

    private void finishAssignment(Assignment assignment) {
        if (!activeAssignment.compareAndSet(assignment, null)) return;
        boolean publishReady = false;
        synchronized (stateLock) {
            if (state == WorkerState.BUSY) {
                state = WorkerState.READY;
                publishReady = true;
            }
        }
        /* 
        System.out.printf("Managed Python request finished: workerId=%s, generation=%d, pid=%d, requestId=%s%n",
                workerId, generation, workerPid, assignment.request().requestId());
        */
                if (publishReady) listener.onReady(this);
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
                Assignment assignment = activeAssignment.get();
                if (assignment == null || !assignment.request().requestId().equals(requestId)) continue;
                if ("response".equals(frame.type())) {
                    int status = numberValue(frame.metadata(), "status");
                    assignment.completion().complete(new PythonCallResponse(
                            status, responseHeaders(frame.metadata().get("headers")), frame.body()));
                } else if ("error".equals(frame.type())) {
                    assignment.completion().completeExceptionally(new IOException(
                            "Managed Python Runtime error: "
                                    + frame.metadata().getOrDefault("message", "unknown error")));
                } else {
                    throw new IOException("Unexpected Python runtime message: " + frame.type());
                }
            }
        } catch (Exception failure) {
            if (state != WorkerState.DRAINING && state != WorkerState.STOPPED) {
                markUnhealthy(asException("Managed Python worker connection failed", failure));
            }
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
        synchronized (outputLock) {
            PythonRuntimeProtocol.writeFrame(channel, metadata, request.body(), config.maxFrameBytes());
        }
    }

    private void gracefulStop() {
        try {
            long remaining = remainingNanos(shutdownDeadlineNs);
            if (remaining > 0 && channel.isOpen()) {
                synchronized (outputLock) {
                    PythonRuntimeProtocol.writeFrame(channel, Map.of(
                            "protocolVersion", PythonRuntimeProtocol.VERSION,
                            "type", "shutdown"), new byte[0], config.maxFrameBytes());
                }
                remaining = remainingNanos(shutdownDeadlineNs);
                if (remaining > 0) shutdownAcknowledged.await(remaining, TimeUnit.NANOSECONDS);
            }
        } catch (Exception ignored) {
            // The common-deadline cleanup below is the deterministic fallback.
        } finally {
            synchronized (stateLock) {
                state = WorkerState.STOPPED;
            }
            cleanup(false, shutdownDeadlineNs);
        }
    }

    private void markUnhealthy(Exception failure) {
        boolean publish;
        synchronized (stateLock) {
            if (state == WorkerState.UNHEALTHY || state == WorkerState.STOPPED
                    || state == WorkerState.DRAINING) return;
            state = WorkerState.UNHEALTHY;
            publish = failurePublished.compareAndSet(false, true);
        }
        Assignment assignment = activeAssignment.getAndSet(null);
        if (assignment != null) assignment.completion().completeExceptionally(failure);
        requestRunner.interrupt();
        closeQuietly(channel);
        if (process.isAlive()) process.destroy();
        if (publish) {
            System.err.printf("Managed Python worker unhealthy: workerId=%s, generation=%d, pid=%d, failure=%s%n",
                    workerId, generation, workerPid, failure.getMessage());
            listener.onUnhealthy(this, failure);
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
            deleteSocket(socketPath);
            synchronized (stateLock) {
                state = WorkerState.STOPPED;
            }
            stopped.countDown();
            /* 
            System.out.printf("Managed Python worker stopped: workerId=%s, generation=%d, pid=%d, socketRemoved=%s%n",
                    workerId, generation, workerPid, !Files.exists(socketPath));
            */
            }
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
        return failure instanceof Exception exception ? new IOException(message, exception) : new IOException(message, failure);
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
