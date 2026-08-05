package com.example.baseline.utils.python;

import com.example.baseline.utils.config.ApplicationConfig.ManagedPythonRuntimeConfig;

import java.io.IOException;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

final class ManagedPythonRuntime implements PythonCallExecutor {
    private final ManagedPythonRuntimeConfig config;
    private final Path runtimeDirectory;
    private final Path socketPath;
    private final Process process;
    private final SocketChannel socketChannel;
    private final ArrayBlockingQueue<Submission> admissionQueue;
    private final ConcurrentHashMap<String, CompletableFuture<PythonCallResponse>> pending = new ConcurrentHashMap<>();
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Object outputLock = new Object();
    private final Object dispatchStateLock = new Object();
    private final AtomicReference<Submission> activeSubmission = new AtomicReference<>();
    private final CountDownLatch shutdownAcknowledged = new CountDownLatch(1);
    private final CountDownLatch dispatcherStopped = new CountDownLatch(1);
    private final Thread dispatcherThread;
    private final Thread responseReaderThread;

    private ManagedPythonRuntime(
            ManagedPythonRuntimeConfig config,
            Path runtimeDirectory,
            Path socketPath,
            Process process,
            SocketChannel socketChannel
    ) {
        this.config = config;
        this.runtimeDirectory = runtimeDirectory;
        this.socketPath = socketPath;
        this.process = process;
        this.socketChannel = socketChannel;
        this.admissionQueue = new ArrayBlockingQueue<>(config.queueCapacity(), true);
        this.dispatcherThread = new Thread(this::dispatchLoop, "managed-python-dispatcher");
        this.responseReaderThread = new Thread(this::responseLoop, "managed-python-response-reader");
        this.dispatcherThread.start();
        this.responseReaderThread.start();
    }

    static ManagedPythonRuntime start(ManagedPythonRuntimeConfig config) throws Exception {
        Path applicationDirectory = Path.of(config.applicationDirectory());
        if (!Files.isDirectory(applicationDirectory)) {
            throw new IllegalArgumentException("Managed Python application directory does not exist: " + applicationDirectory);
        }
        validateExecutable(config.pythonExecutable());

        Path udsParent = Path.of(config.udsDirectory());
        Files.createDirectories(udsParent);
        Path runtimeDirectory = Files.createTempDirectory(udsParent, "runtime-");
        setPrivatePermissionsWhenSupported(runtimeDirectory);
        Path socketPath = runtimeDirectory.resolve("worker.sock");
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
                    Integer.toString(config.maxFrameBytes())
            )
                    .directory(applicationDirectory.toFile())
                    .inheritIO()
                    .start();

            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.startupTimeoutMs());
            channel = connect(socketPath, process, deadline);
            PythonRuntimeProtocol.Frame ready = readStartupFrame(channel, config.maxFrameBytes(), deadline);
            if (!"ready".equals(ready.type())) {
                throw new IOException("Expected ready message, received: " + ready.type());
            }
            Object workerPid = ready.metadata().get("workerPid");
            System.out.printf("Managed Python Runtime ready: workerPid=%s, socket=%s%n", workerPid, socketPath);
            return new ManagedPythonRuntime(
                    config, runtimeDirectory, socketPath, process, channel);
        } catch (Exception failure) {
            if (channel != null) closeQuietly(channel);
            stopProcess(process, config.shutdownTimeoutMs());
            deleteRuntimeFiles(socketPath, runtimeDirectory);
            throw failure;
        }
    }

    @Override
    public PythonCallResponse call(PythonCallRequest request) throws Exception {
        if (!running.get()) throw new IllegalStateException("Managed Python Runtime is not accepting requests");
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.requestTimeoutMs());
        Submission submission = new Submission(request, deadline);
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0 || !admissionQueue.offer(submission, remaining, TimeUnit.NANOSECONDS)) {
            throw new TimeoutException("Managed Python Runtime admission queue timed out");
        }
        if (!running.get() && admissionQueue.remove(submission)) {
            submission.completion.completeExceptionally(shutdownFailure());
        }
        try {
            remaining = deadline - System.nanoTime();
            if (remaining <= 0) throw new TimeoutException("Managed Python Runtime request timed out");
            return submission.completion.get(remaining, TimeUnit.NANOSECONDS);
        } catch (TimeoutException exception) {
            admissionQueue.remove(submission);
            pending.remove(request.requestId(), submission.completion);
            submission.completion.completeExceptionally(exception);
            throw exception;
        } catch (ExecutionException exception) {
            Throwable cause = exception.getCause();
            if (cause instanceof Exception checked) throw checked;
            throw new IllegalStateException("Managed Python Runtime request failed", cause);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw exception;
        }
    }

    private void dispatchLoop() {
        Submission submission = null;
        try {
            while (true) {
                synchronized (dispatchStateLock) {
                    if (!running.get()) break;
                    submission = admissionQueue.poll();
                    if (submission != null) activeSubmission.set(submission);
                }
                if (submission == null) {
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException exception) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                    continue;
                }
                try {
                    if (submission.completion.isDone()) continue;
                    if (System.nanoTime() >= submission.deadlineNs) {
                        submission.completion.completeExceptionally(
                                new TimeoutException("Managed Python Runtime request timed out in queue"));
                        continue;
                    }
                    synchronized (dispatchStateLock) {
                        if (!running.get()) {
                            submission.completion.completeExceptionally(shutdownFailure());
                            continue;
                        }
                        if (pending.putIfAbsent(submission.request.requestId(), submission.completion) != null) {
                            submission.completion.completeExceptionally(new IllegalStateException(
                                    "Duplicate pending request ID: " + submission.request.requestId()));
                            continue;
                        }
                        writeRequest(submission.request);
                        submission.transmitted = true;
                    }
                    long remaining = submission.deadlineNs - System.nanoTime();
                    if (remaining <= 0) throw new TimeoutException("Managed Python Runtime request timed out");
                    submission.completion.get(remaining, TimeUnit.NANOSECONDS);
                } catch (InterruptedException exception) {
                    submission.completion.completeExceptionally(
                            new IllegalStateException("Managed Python Runtime dispatcher was interrupted", exception));
                    Thread.currentThread().interrupt();
                    break;
                } catch (ExecutionException ignored) {
                    // The response reader or shutdown path recorded the request-specific failure.
                } catch (Exception exception) {
                    submission.completion.completeExceptionally(exception);
                } finally {
                    pending.remove(submission.request.requestId(), submission.completion);
                    activeSubmission.compareAndSet(submission, null);
                    submission = null;
                }
            }
        } finally {
            if (submission != null) {
                submission.completion.completeExceptionally(shutdownFailure());
                pending.remove(submission.request.requestId(), submission.completion);
                activeSubmission.compareAndSet(submission, null);
            }
            dispatcherStopped.countDown();
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
        List<List<String>> headers = request.headers().entrySet().stream()
                .map(entry -> List.of(entry.getKey(), entry.getValue()))
                .toList();
        metadata.put("headers", headers);
        synchronized (outputLock) {
            PythonRuntimeProtocol.writeFrame(socketChannel, metadata, request.body(), config.maxFrameBytes());
        }
    }

    private void responseLoop() {
        try {
            while (true) {
                PythonRuntimeProtocol.Frame frame = PythonRuntimeProtocol.readFrame(socketChannel, config.maxFrameBytes());
                if ("shutdown_ack".equals(frame.type())) {
                    shutdownAcknowledged.countDown();
                    return;
                }
                String requestId = stringValue(frame.metadata(), "requestId");
                CompletableFuture<PythonCallResponse> completion = pending.get(requestId);
                if (completion == null) continue;
                if ("response".equals(frame.type())) {
                    int status = numberValue(frame.metadata(), "status");
                    completion.complete(new PythonCallResponse(
                            status, responseHeaders(frame.metadata().get("headers")), frame.body()));
                } else if ("error".equals(frame.type())) {
                    completion.completeExceptionally(new IOException(
                            "Managed Python Runtime error: " + frame.metadata().getOrDefault("message", "unknown error")));
                } else {
                    throw new IOException("Unexpected Python runtime message: " + frame.type());
                }
            }
        } catch (Exception exception) {
            if (running.get()) failAll(new IOException("Managed Python Runtime connection failed", exception));
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, List<String>> responseHeaders(Object value) throws IOException {
        if (!(value instanceof List<?> entries)) throw new IOException("Response headers are invalid");
        Map<String, List<String>> result = new LinkedHashMap<>();
        for (Object entry : entries) {
            if (!(entry instanceof List<?> pair) || pair.size() != 2) throw new IOException("Response header is invalid");
            String name = String.valueOf(pair.get(0));
            String headerValue = String.valueOf(pair.get(1));
            result.computeIfAbsent(name, ignored -> new ArrayList<>()).add(headerValue);
        }
        return result;
    }

    @Override
    public void close() {
        if (!running.compareAndSet(true, false)) return;
        long shutdownDeadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.shutdownTimeoutMs());
        IllegalStateException shutdownFailure = shutdownFailure();
        Submission active;
        synchronized (dispatchStateLock) {
            Submission queued;
            while ((queued = admissionQueue.poll()) != null) {
                queued.completion.completeExceptionally(shutdownFailure);
            }
            active = activeSubmission.get();
            if (active != null && !active.transmitted) {
                active.completion.completeExceptionally(shutdownFailure);
            }
        }

        if (active != null && active.transmitted && !active.completion.isDone()) {
            awaitActiveCompletion(active, shutdownDeadline);
        }
        if (active != null && !active.completion.isDone()) {
            TimeoutException timeout = new TimeoutException(
                    "Managed Python Runtime active request exceeded the shutdown timeout");
            active.completion.completeExceptionally(timeout);
            pending.remove(active.request.requestId(), active.completion);
        }
        failAll(shutdownFailure);

        try {
            if (remainingNanos(shutdownDeadline) > 0 && socketChannel.isOpen()) {
                Map<String, Object> metadata = Map.of(
                        "protocolVersion", PythonRuntimeProtocol.VERSION,
                        "type", "shutdown"
                );
                synchronized (outputLock) {
                    PythonRuntimeProtocol.writeFrame(socketChannel, metadata, new byte[0], config.maxFrameBytes());
                }
                long remaining = remainingNanos(shutdownDeadline);
                if (remaining > 0) shutdownAcknowledged.await(remaining, TimeUnit.NANOSECONDS);
            }
        } catch (Exception ignored) {
            // Process termination below is the deterministic fallback.
        } finally {
            closeQuietly(socketChannel);
            awaitLatch(dispatcherStopped, shutdownDeadline);
            joinUntil(responseReaderThread, shutdownDeadline);
            stopProcessUntil(process, shutdownDeadline);
            deleteRuntimeFiles(socketPath, runtimeDirectory);
            System.out.printf("Managed Python Runtime stopped: workerPid=%d, socketRemoved=%s%n",
                    process.pid(), !Files.exists(socketPath));
        }
    }

    private static void awaitActiveCompletion(Submission active, long shutdownDeadline) {
        long remaining = remainingNanos(shutdownDeadline);
        if (remaining <= 0) return;
        try {
            active.completion.get(remaining, TimeUnit.NANOSECONDS);
        } catch (ExecutionException | TimeoutException ignored) {
            // Completion state is inspected by close() after the drain opportunity.
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
    }

    private void failAll(Exception exception) {
        pending.forEach((requestId, completion) -> completion.completeExceptionally(exception));
        pending.clear();
    }

    private static IllegalStateException shutdownFailure() {
        return new IllegalStateException("Managed Python Runtime is shutting down");
    }

    private static SocketChannel connect(Path socketPath, Process process, long deadline) throws Exception {
        Exception lastFailure = null;
        while (System.nanoTime() < deadline) {
            if (!process.isAlive()) throw new IOException("Python worker exited during startup with code " + process.exitValue());
            if (Files.exists(socketPath)) {
                SocketChannel channel = SocketChannel.open(StandardProtocolFamily.UNIX);
                try {
                    channel.connect(UnixDomainSocketAddress.of(socketPath));
                    return channel;
                } catch (Exception exception) {
                    lastFailure = exception;
                    closeQuietly(channel);
                }
            }
            Thread.sleep(25);
        }
        throw new TimeoutException("Timed out connecting to Python runtime socket" +
                (lastFailure == null ? "" : ": " + lastFailure.getMessage()));
    }

    private static PythonRuntimeProtocol.Frame readStartupFrame(
            SocketChannel channel, int maxFrameBytes, long deadline) throws Exception {
        CompletableFuture<PythonRuntimeProtocol.Frame> future = CompletableFuture.supplyAsync(() -> {
            try {
                return PythonRuntimeProtocol.readFrame(channel, maxFrameBytes);
            } catch (IOException exception) {
                throw new IllegalStateException(exception);
            }
        });
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0) throw new TimeoutException("Python worker readiness timed out");
        return future.get(remaining, TimeUnit.NANOSECONDS);
    }

    private static void validateExecutable(String executable) {
        if (executable.contains("/") || executable.contains("\\")) {
            Path path = Path.of(executable);
            if (!Files.isRegularFile(path)) {
                throw new IllegalArgumentException("Python executable does not exist: " + executable);
            }
        }
    }

    private static void setPrivatePermissionsWhenSupported(Path directory) {
        try {
            Files.setPosixFilePermissions(directory, Set.of(
                    PosixFilePermission.OWNER_READ,
                    PosixFilePermission.OWNER_WRITE,
                    PosixFilePermission.OWNER_EXECUTE));
        } catch (UnsupportedOperationException | IOException ignored) {
            // POSIX permissions are unavailable on some development hosts.
        }
    }

    private static String stringValue(Map<String, Object> metadata, String name) throws IOException {
        Object value = metadata.get(name);
        if (!(value instanceof String text) || text.isBlank()) throw new IOException("Missing protocol field: " + name);
        return text;
    }

    private static int numberValue(Map<String, Object> metadata, String name) throws IOException {
        Object value = metadata.get(name);
        if (!(value instanceof Number number)) throw new IOException("Missing numeric protocol field: " + name);
        return number.intValue();
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

    private static void stopProcessUntil(Process process, long deadlineNs) {
        if (process == null || !process.isAlive()) return;
        try {
            long remaining = remainingNanos(deadlineNs);
            if (remaining > 0 && process.waitFor(remaining, TimeUnit.NANOSECONDS)) return;
            process.destroy();
            remaining = remainingNanos(deadlineNs);
            if (remaining > 0 && process.waitFor(remaining, TimeUnit.NANOSECONDS)) return;
            process.destroyForcibly();
        } catch (InterruptedException exception) {
            process.destroyForcibly();
            Thread.currentThread().interrupt();
        }
    }

    private static void deleteRuntimeFiles(Path socketPath, Path runtimeDirectory) {
        try {
            Files.deleteIfExists(socketPath);
            Files.deleteIfExists(runtimeDirectory);
        } catch (IOException exception) {
            System.err.printf("Unable to remove managed runtime path %s: %s%n", runtimeDirectory, exception.getMessage());
        }
    }

    private static void closeQuietly(AutoCloseable closeable) {
        try {
            closeable.close();
        } catch (Exception ignored) {
            // Best-effort cleanup after the primary failure.
        }
    }

    private static void joinUntil(Thread thread, long deadlineNs) {
        try {
            long remaining = remainingNanos(deadlineNs);
            if (remaining > 0) TimeUnit.NANOSECONDS.timedJoin(thread, remaining);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
    }

    private static void awaitLatch(CountDownLatch latch, long deadlineNs) {
        try {
            long remaining = remainingNanos(deadlineNs);
            if (remaining > 0) latch.await(remaining, TimeUnit.NANOSECONDS);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
    }

    private static long remainingNanos(long deadlineNs) {
        return Math.max(0, deadlineNs - System.nanoTime());
    }

    private static final class Submission {
        private final PythonCallRequest request;
        private final long deadlineNs;
        private final CompletableFuture<PythonCallResponse> completion = new CompletableFuture<>();
        private volatile boolean transmitted;

        private Submission(PythonCallRequest request, long deadlineNs) {
            this.request = request;
            this.deadlineNs = deadlineNs;
        }
    }
}
