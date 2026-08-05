package com.example.baseline.utils.python;

import com.example.baseline.utils.config.ApplicationConfig.ManagedPythonRuntimeConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

final class ManagedPythonRuntime implements PythonCallExecutor, ManagedPythonWorker.Listener {
    private static final Logger LOGGER = LoggerFactory.getLogger(ManagedPythonRuntime.class);
    private static final int MAX_FAILURE_MESSAGE = 512;
    private enum PoolState {
        STARTING,
        RUNNING,
        DEGRADED,
        UNAVAILABLE,
        DRAINING,
        STOPPED
    }

    private final ManagedPythonRuntimeConfig config;
    private final Path runtimeDirectory;
    private final ArrayBlockingQueue<Submission> admissionQueue;
    private final ArrayBlockingQueue<ManagedPythonWorker> availableWorkers;
    private final List<WorkerSlot> slots;
    private final AtomicBoolean accepting = new AtomicBoolean();
    private final AtomicBoolean closeStarted = new AtomicBoolean();
    private final AtomicReference<Submission> dispatcherOwned = new AtomicReference<>();
    private final AtomicInteger healthyWorkers = new AtomicInteger();
    private final CountDownLatch firstAttempts;
    private final CountDownLatch initialDecision = new CountDownLatch(1);
    private final CountDownLatch dispatcherStopped = new CountDownLatch(1);
    private final ScheduledThreadPoolExecutor healthScheduler;
    private final RuntimeMetrics metrics = new RuntimeMetrics();
    private final AtomicReference<ManagedPythonRuntimeSnapshot.FailureSummary> lastFailure =
            new AtomicReference<>();
    private final Object stateLock = new Object();
    private final Object dispatchStateLock = new Object();
    private volatile PoolState state = PoolState.STARTING;
    private volatile long initialStartupDeadlineNs;
    private Thread dispatcherThread;
    private final long createdAtNs = System.nanoTime();

    private ManagedPythonRuntime(ManagedPythonRuntimeConfig config, Path runtimeDirectory) {
        this.config = config;
        this.runtimeDirectory = runtimeDirectory;
        this.admissionQueue = new ArrayBlockingQueue<>(config.queueCapacity(), true);
        this.availableWorkers = new ArrayBlockingQueue<>(config.workerCount(), true);
        this.healthScheduler = new ScheduledThreadPoolExecutor(1, runnable -> {
            Thread thread = new Thread(runnable, "managed-python-health-scheduler");
            thread.setDaemon(true);
            return thread;
        });
        this.healthScheduler.setRemoveOnCancelPolicy(true);
        this.healthScheduler.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        this.healthScheduler.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        this.firstAttempts = new CountDownLatch(config.workerCount());
        List<WorkerSlot> createdSlots = new ArrayList<>(config.workerCount());
        for (int index = 1; index <= config.workerCount(); index++) {
            createdSlots.add(new WorkerSlot("worker-" + index));
        }
        this.slots = List.copyOf(createdSlots);
    }

    static ManagedPythonRuntime start(ManagedPythonRuntimeConfig config) throws Exception {
        config.validate();
        Path applicationDirectory = Path.of(config.applicationDirectory());
        if (!Files.isDirectory(applicationDirectory) || !Files.isReadable(applicationDirectory)) {
            throw new IllegalArgumentException(
                    "Managed Python application directory is not readable: " + applicationDirectory);
        }
        validateExecutable(config.pythonExecutable());

        Path udsParent = Path.of(config.udsDirectory()).toAbsolutePath().normalize();
        validateSocketPathLength(udsParent);
        Files.createDirectories(udsParent);
        Path runtimeDirectory = Files.createTempDirectory(udsParent, "runtime-pool-");
        setAndVerifyPrivatePermissions(runtimeDirectory);
        ManagedPythonRuntime runtime = new ManagedPythonRuntime(config, runtimeDirectory);
        try {
            runtime.startSlots();
            return runtime;
        } catch (Exception failure) {
            runtime.abortStartup();
            throw failure;
        }
    }

    private void startSlots() throws Exception {
        LOGGER.info("event=managed_python_pool_starting configuredWorkers={} maxInFlightPerWorker={} directory={}",
                config.workerCount(), config.maxInFlightPerWorker(), runtimeDirectory);
        initialStartupDeadlineNs = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(config.startupTimeoutMs());
        for (WorkerSlot slot : slots) slot.start();
        if (!firstAttempts.await(remainingNanos(initialStartupDeadlineNs), TimeUnit.NANOSECONDS)) {
            recordFailure(ManagedPythonFailureCategory.STARTUP_TIMEOUT, null,
                    "Worker pool initial attempts exceeded the shared startup deadline", null);
            throw new TimeoutException("Managed Python worker pool startup did not complete within the configured bound");
        }
        int ready = healthyWorkers.get();
        if (ready == 0) {
            throw new IllegalStateException("No Managed Python worker completed its initial startup attempt");
        }
        accepting.set(true);
        updatePoolState();
        dispatcherThread = new Thread(this::dispatchLoop, "managed-python-pool-dispatcher");
        dispatcherThread.start();
        startHealthScheduler();
        initialDecision.countDown();
        LOGGER.info("event=managed_python_pool_ready poolState={} readyWorkers={} configuredWorkers={} directory={}",
                state, ready, config.workerCount(), runtimeDirectory);
    }

    @Override
    public PythonCallResponse call(PythonCallRequest request) throws Exception {
        if (!accepting.get()) throw unavailableOrShutdownFailure();
        if (healthyWorkers.get() == 0 && allSlotsExhausted()) throw unavailableFailure();
        long admittedAt = System.nanoTime();
        long queueDeadline = admittedAt + TimeUnit.MILLISECONDS.toNanos(config.queueTimeoutMs());
        long callerDeadline = queueDeadline + TimeUnit.MILLISECONDS.toNanos(config.requestTimeoutMs());
        Submission submission = new Submission(request, admittedAt, queueDeadline);
        long remaining = remainingNanos(queueDeadline);
        if (remaining <= 0 || !admissionQueue.offer(submission, remaining, TimeUnit.NANOSECONDS)) {
            metrics.queueFullObservations.increment();
            metrics.queueTimeouts.increment();
            recordFailure(ManagedPythonFailureCategory.QUEUE_SATURATED, null,
                    "Admission queue remained full until the queue deadline", null);
            throw new TimeoutException("Managed Python Runtime admission queue timed out");
        }
        metrics.requestsAdmitted.increment();
        if (!accepting.get() && admissionQueue.remove(submission)) {
            submission.completion.completeExceptionally(shutdownFailure());
        }
        try {
            remaining = remainingNanos(callerDeadline);
            if (remaining <= 0) throw new TimeoutException("Managed Python Runtime request timed out");
            PythonCallResponse response = submission.completion.get(remaining, TimeUnit.NANOSECONDS);
            metrics.requestsCompleted.increment();
            return response;
        } catch (TimeoutException waitTimeout) {
            TimeoutException timeout = new TimeoutException("Managed Python Runtime request timed out");
            timeout.initCause(waitTimeout);
            admissionQueue.remove(submission);
            boolean timeoutWon = submission.completion.completeExceptionally(timeout);
            ManagedPythonWorker worker = submission.assignedWorker.get();
            ManagedPythonWorker.Assignment assignment = submission.assignment;
            if (worker != null && assignment != null) worker.requestTimedOut(assignment, timeout);
            if (!timeoutWon) {
                try {
                    PythonCallResponse response = submission.completion.get();
                    metrics.requestsCompleted.increment();
                    return response;
                } catch (ExecutionException completedFailure) {
                    metrics.requestsFailed.increment();
                    Throwable cause = completedFailure.getCause();
                    if (cause instanceof Exception checked) throw checked;
                    throw new IllegalStateException("Managed Python Runtime request failed", cause);
                }
            }
            metrics.requestsFailed.increment();
            throw timeout;
        } catch (ExecutionException failure) {
            metrics.requestsFailed.increment();
            Throwable cause = failure.getCause();
            if (cause instanceof Exception checked) throw checked;
            throw new IllegalStateException("Managed Python Runtime request failed", cause);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw interrupted;
        }
    }

    private void dispatchLoop() {
        Submission submission = null;
        try {
            while (accepting.get()) {
                submission = admissionQueue.poll(50, TimeUnit.MILLISECONDS);
                if (submission == null) continue;
                dispatcherOwned.set(submission);
                if (submission.completion.isDone()) {
                    dispatcherOwned.compareAndSet(submission, null);
                    submission = null;
                    continue;
                }
                assignHead(submission);
                dispatcherOwned.compareAndSet(submission, null);
                submission = null;
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            if (submission != null) {
                submission.completion.completeExceptionally(
                        new IllegalStateException("Managed Python pool dispatcher was interrupted", interrupted));
            }
        } finally {
            if (submission != null) submission.completion.completeExceptionally(shutdownFailure());
            Submission owned = dispatcherOwned.getAndSet(null);
            if (owned != null) owned.completion.completeExceptionally(shutdownFailure());
            dispatcherStopped.countDown();
        }
    }

    private void assignHead(Submission submission) throws InterruptedException {
        while (accepting.get() && !submission.completion.isDone()) {
            long remaining = remainingNanos(submission.queueDeadlineNs);
            if (remaining <= 0) {
                metrics.queueTimeouts.increment();
                recordFailure(ManagedPythonFailureCategory.QUEUE_TIMEOUT, null,
                        "FIFO submission exceeded its worker-assignment deadline", null);
                submission.completion.completeExceptionally(
                        new TimeoutException("Managed Python Runtime queue wait timed out"));
                return;
            }
            if (healthyWorkers.get() == 0 && allSlotsExhausted()) {
                submission.completion.completeExceptionally(unavailableFailure());
                return;
            }
            ManagedPythonWorker worker = availableWorkers.poll(
                    Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(50)), TimeUnit.NANOSECONDS);
            if (worker == null) continue;
            worker.consumeAvailabilityPublication();
            WorkerSlot slot = slotFor(worker.workerId());
            if (!slot.isCurrent(worker) || !worker.isRunning()) continue;

            ManagedPythonWorker.Assignment assignment = new ManagedPythonWorker.Assignment(
                    submission.request, submission.completion);
            synchronized (dispatchStateLock) {
                if (!accepting.get() || submission.completion.isDone()) {
                    if (accepting.get()) publishAvailable(worker);
                    if (!submission.completion.isDone()) {
                        submission.completion.completeExceptionally(shutdownFailure());
                    }
                    return;
                }
                submission.assignment = assignment;
                submission.assignedWorker.set(worker);
                if (!worker.tryReserveAndEnqueue(assignment)) {
                    submission.assignment = null;
                    submission.assignedWorker.compareAndSet(worker, null);
                    publishAvailable(worker);
                    continue;
                }
                metrics.queueWait.record(System.nanoTime() - submission.admittedAtNs);
            }
            publishAvailable(worker);
            return;
        }
        if (!submission.completion.isDone()) submission.completion.completeExceptionally(shutdownFailure());
    }

    @Override
    public void onCapacityAvailable(ManagedPythonWorker worker) {
        publishAvailable(worker);
    }

    private void publishAvailable(ManagedPythonWorker worker) {
        WorkerSlot slot = slotFor(worker.workerId());
        if (!slot.isCurrent(worker) || !worker.isRunning()) return;
        if (!accepting.get() && initialDecision.getCount() == 0) {
            worker.beginDrain(System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.shutdownTimeoutMs()));
            return;
        }
        if (!worker.tryMarkAvailabilityPublished()) return;
        if (!availableWorkers.offer(worker)) {
            worker.clearAvailabilityPublication();
            worker.fail(new IOException("Managed Python Runtime availability queue rejected a worker token"));
        }
    }

    @Override
    public void onUnhealthy(
            ManagedPythonWorker worker,
            ManagedPythonFailureCategory category,
            Exception failure) {
        worker.clearAvailabilityPublication();
        availableWorkers.remove(worker);
        if (category == ManagedPythonFailureCategory.HEALTH_CHECK_TIMEOUT
                || category == ManagedPythonFailureCategory.HEALTH_CHECK_PROTOCOL_FAILURE) {
            metrics.healthChecksFailed.increment();
            LOGGER.warn("event=managed_python_health_check_failed workerId={} generation={} pid={} "
                            + "failureCategory={} failure={}",
                    worker.workerId(), worker.generation(), worker.pid(), category, boundedMessage(failure));
        } else {
            metrics.workerCrashes.increment();
        }
        recordFailure(category, worker, "Worker generation became unhealthy", failure);
        WorkerSlot slot = slotFor(worker.workerId());
        if (slot.markUnhealthy(worker)) {
            healthyWorkers.decrementAndGet();
            updatePoolState();
        }
    }

    @Override
    public void onRequestTerminal(
            ManagedPythonWorker worker,
            ManagedPythonWorker.Assignment assignment,
            boolean successful,
            ManagedPythonFailureCategory category) {
        if (assignment.assignedAtNs() > 0) {
            metrics.requestExecution.record(System.nanoTime() - assignment.assignedAtNs());
        }
        if (category == ManagedPythonFailureCategory.REQUEST_TIMEOUT_PRE_TRANSMISSION) {
            metrics.preTransmissionTimeouts.increment();
        } else if (category == ManagedPythonFailureCategory.REQUEST_TIMEOUT_POST_TRANSMISSION) {
            metrics.postTransmissionTimeouts.increment();
        }
    }

    @Override
    public void onHealthCheckSent(ManagedPythonWorker worker, String healthCheckId) {
        metrics.healthChecksSent.increment();
    }

    @Override
    public void onHealthCheckSucceeded(
            ManagedPythonWorker worker, String healthCheckId, long latencyNanos) {
        metrics.healthChecksSucceeded.increment();
    }

    @Override
    public void onForcedTermination(ManagedPythonWorker worker) {
        metrics.forcedTerminations.increment();
        recordFailure(ManagedPythonFailureCategory.FORCED_TERMINATION, worker,
                "Worker required forced termination", null);
    }

    @Override
    public void onCleanupFailure(ManagedPythonWorker worker, String message, Exception failure) {
        metrics.cleanupFailures.increment();
        recordFailure(ManagedPythonFailureCategory.CLEANUP_FAILURE, worker, message, failure);
    }

    @Override
    public void close() {
        if (!closeStarted.compareAndSet(false, true)) return;
        long shutdownStarted = System.nanoTime();
        long deadline = shutdownStarted + TimeUnit.MILLISECONDS.toNanos(config.shutdownTimeoutMs());
        LOGGER.info("event=managed_python_pool_shutdown_started poolState={} queueDepth={} healthyWorkers={}",
                state, admissionQueue.size(), healthyWorkers.get());
        synchronized (dispatchStateLock) {
            accepting.set(false);
        }
        synchronized (stateLock) {
            state = PoolState.DRAINING;
        }
        initialDecision.countDown();
        stopHealthScheduler(deadline);
        IllegalStateException closing = shutdownFailure();

        Submission queued;
        while ((queued = admissionQueue.poll()) != null) queued.completion.completeExceptionally(closing);
        Submission owned = dispatcherOwned.get();
        if (owned != null) owned.completion.completeExceptionally(closing);

        List<ManagedPythonWorker> workers = currentWorkers();
        availableWorkers.clear();
        for (ManagedPythonWorker worker : workers) {
            worker.clearAvailabilityPublication();
            worker.beginDrain(deadline);
        }
        awaitLatch(dispatcherStopped, deadline);
        for (ManagedPythonWorker worker : workers) worker.awaitStopped(deadline);
        TimeoutException timeout = new TimeoutException(
                "Managed Python Runtime pool exceeded the common shutdown timeout");
        for (ManagedPythonWorker worker : workers) {
            if (!worker.isStopped()) {
                recordFailure(ManagedPythonFailureCategory.SHUTDOWN_TIMEOUT, worker,
                        "Worker exceeded the common shutdown deadline", timeout);
                worker.forceStop(timeout);
            }
        }
        for (WorkerSlot slot : slots) slot.interruptSupervisor();
        for (WorkerSlot slot : slots) slot.joinSupervisor(deadline);
        deleteRuntimeDirectory();
        synchronized (stateLock) {
            state = PoolState.STOPPED;
        }
        metrics.poolShutdown.record(System.nanoTime() - shutdownStarted);
        LOGGER.info("event=managed_python_pool_shutdown_completed workers={} durationNs={} "
                        + "directoryRemoved={} forcedTerminations={} cleanupFailures={}",
                workers.size(), System.nanoTime() - shutdownStarted, !Files.exists(runtimeDirectory),
                metrics.forcedTerminations.sum(), metrics.cleanupFailures.sum());
    }

    private void abortStartup() {
        closeStarted.set(true);
        accepting.set(false);
        synchronized (stateLock) {
            state = PoolState.DRAINING;
        }
        initialDecision.countDown();
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.shutdownTimeoutMs());
        stopHealthScheduler(deadline);
        for (ManagedPythonWorker worker : currentWorkers()) worker.forceStop(shutdownFailure());
        for (WorkerSlot slot : slots) slot.interruptSupervisor();
        for (WorkerSlot slot : slots) slot.joinSupervisor(deadline);
        deleteRuntimeDirectory();
        synchronized (stateLock) {
            state = PoolState.STOPPED;
        }
    }

    ManagedPythonRuntimeSnapshot snapshot() {
        List<ManagedPythonRuntimeSnapshot.Worker> workerSnapshots = new ArrayList<>(slots.size());
        int responsive = 0;
        int restarting = 0;
        int exhausted = 0;
        int totalInFlight = 0;
        int availableCapacity = 0;
        for (WorkerSlot slot : slots) {
            WorkerSlot.SlotSnapshot slotSnapshot = slot.slotSnapshot();
            if (slotSnapshot.restarting()) restarting++;
            if (slotSnapshot.exhausted()) exhausted++;
            ManagedPythonWorker worker = slotSnapshot.worker();
            if (worker == null) {
                workerSnapshots.add(new ManagedPythonRuntimeSnapshot.Worker(
                        slot.workerId, slotSnapshot.generation(), -1, "ABSENT", false,
                        false, false, 0, config.maxInFlightPerWorker(), false,
                        0, 0, slotSnapshot.restarting(), slotSnapshot.exhausted()));
                continue;
            }
            ManagedPythonRuntimeSnapshot.Worker snapshot = worker.snapshot(
                    slotSnapshot.restarting(), slotSnapshot.exhausted());
            workerSnapshots.add(snapshot);
            if (snapshot.responsive()) responsive++;
            totalInFlight += snapshot.inFlight();
            availableCapacity += snapshot.dispatchEligible()
                    ? Math.max(0, snapshot.maximumInFlight() - snapshot.inFlight()) : 0;
        }
        PoolState observed = state;
        return new ManagedPythonRuntimeSnapshot(
                observed.name(),
                accepting.get(),
                responsive > 0,
                responsive == config.workerCount(),
                config.workerCount(),
                responsive,
                restarting,
                exhausted,
                admissionQueue.size(),
                config.queueCapacity(),
                totalInFlight,
                config.workerCount() * config.maxInFlightPerWorker(),
                availableCapacity,
                metrics.snapshot(),
                lastFailure.get(),
                workerSnapshots);
    }

    private void startHealthScheduler() {
        if (!config.healthCheckEnabled()) {
            healthScheduler.shutdown();
            return;
        }
        long sweepMillis = Math.max(50,
                Math.min(250, Math.max(1, config.healthCheckTimeoutMs() / 4)));
        healthScheduler.scheduleWithFixedDelay(this::healthSweep,
                sweepMillis, sweepMillis, TimeUnit.MILLISECONDS);
    }

    private void healthSweep() {
        if (closeStarted.get()) return;
        long now = System.nanoTime();
        for (WorkerSlot slot : slots) {
            ManagedPythonWorker worker = slot.currentWorker();
            if (worker != null && slot.isCurrent(worker)) worker.healthSweep(now);
        }
    }

    private void stopHealthScheduler(long deadlineNs) {
        healthScheduler.shutdownNow();
        try {
            long remaining = remainingNanos(deadlineNs);
            if (remaining > 0) healthScheduler.awaitTermination(remaining, TimeUnit.NANOSECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            recordFailure(ManagedPythonFailureCategory.SHUTDOWN_INTERRUPTED, null,
                    "Interrupted while stopping the health scheduler", interrupted);
        }
    }

    private List<ManagedPythonWorker> currentWorkers() {
        List<ManagedPythonWorker> workers = new ArrayList<>();
        for (WorkerSlot slot : slots) {
            ManagedPythonWorker worker = slot.currentWorker();
            if (worker != null) workers.add(worker);
        }
        return workers;
    }

    private WorkerSlot slotFor(String workerId) {
        int index = Integer.parseInt(workerId.substring("worker-".length())) - 1;
        return slots.get(index);
    }

    private boolean allSlotsExhausted() {
        for (WorkerSlot slot : slots) {
            if (!slot.isExhausted()) return false;
        }
        return true;
    }

    private void updatePoolState() {
        if (closeStarted.get()) return;
        int healthy = healthyWorkers.get();
        PoolState next;
        if (healthy == config.workerCount()) next = PoolState.RUNNING;
        else if (healthy > 0) next = PoolState.DEGRADED;
        else next = PoolState.UNAVAILABLE;
        PoolState previous;
        synchronized (stateLock) {
            previous = state;
            if (state != PoolState.DRAINING && state != PoolState.STOPPED) state = next;
        }
        if (previous != next && previous != PoolState.DRAINING && previous != PoolState.STOPPED) {
            if (next == PoolState.UNAVAILABLE) {
                LOGGER.error("event=managed_python_pool_unavailable previousState={} poolState={} healthyWorkers={}",
                        previous, next, healthy);
            } else if (next == PoolState.DEGRADED) {
                LOGGER.warn("event=managed_python_pool_degraded previousState={} poolState={} healthyWorkers={} "
                                + "configuredWorkers={}",
                        previous, next, healthy, config.workerCount());
            } else {
                LOGGER.info("event=managed_python_pool_ready previousState={} poolState={} healthyWorkers={} "
                                + "configuredWorkers={}",
                        previous, next, healthy, config.workerCount());
            }
        }
    }

    private Exception unavailableOrShutdownFailure() {
        if (state == PoolState.UNAVAILABLE && allSlotsExhausted()) return unavailableFailure();
        return shutdownFailure();
    }

    private static IllegalStateException unavailableFailure() {
        return new IllegalStateException("Managed Python Runtime has no available worker capacity");
    }

    private static IllegalStateException shutdownFailure() {
        return new IllegalStateException("Managed Python Runtime is shutting down");
    }

    private void deleteRuntimeDirectory() {
        try {
            Path normalizedParent = Path.of(config.udsDirectory()).toAbsolutePath().normalize();
            Path normalizedRuntime = runtimeDirectory.toAbsolutePath().normalize();
            if (!normalizedRuntime.startsWith(normalizedParent)
                    || normalizedRuntime.equals(normalizedParent)
                    || !normalizedRuntime.getFileName().toString().startsWith("runtime-pool-")) {
                throw new IOException("Refusing to delete an unowned managed runtime path: " + normalizedRuntime);
            }
            if (Files.exists(runtimeDirectory)) {
                try (var paths = Files.list(runtimeDirectory)) {
                    paths.forEach(path -> {
                        try {
                            Files.deleteIfExists(path);
                        } catch (IOException failure) {
                            metrics.cleanupFailures.increment();
                            recordFailure(ManagedPythonFailureCategory.CLEANUP_FAILURE, null,
                                    "Unable to remove managed runtime path " + path, failure);
                        }
                    });
                }
            }
            Files.deleteIfExists(runtimeDirectory);
        } catch (IOException failure) {
            metrics.cleanupFailures.increment();
            recordFailure(ManagedPythonFailureCategory.CLEANUP_FAILURE, null,
                    "Unable to remove managed runtime directory " + runtimeDirectory, failure);
            LOGGER.error("event=managed_python_cleanup_failed resource=runtime_directory path={} failure={}",
                    runtimeDirectory, boundedMessage(failure));
        }
    }

    private static void validateExecutable(String executable) {
        if (executable.contains("/") || executable.contains("\\")) {
            Path path = Path.of(executable);
            if (!Files.isRegularFile(path)) {
                throw new IllegalArgumentException("Python executable does not exist: " + executable);
            }
            if (!Files.isExecutable(path)) {
                throw new IllegalArgumentException("Python executable is not executable: " + executable);
            }
        }
    }

    private static void setAndVerifyPrivatePermissions(Path directory) throws IOException {
        if (Files.getFileAttributeView(directory, PosixFileAttributeView.class) == null) {
            LOGGER.debug("event=managed_python_posix_permissions_unavailable path={}", directory);
            return;
        }
        Set<PosixFilePermission> expected = Set.of(
                PosixFilePermission.OWNER_READ,
                PosixFilePermission.OWNER_WRITE,
                PosixFilePermission.OWNER_EXECUTE);
        Files.setPosixFilePermissions(directory, expected);
        if (!Files.getPosixFilePermissions(directory).equals(expected)) {
            throw new IOException("Managed runtime directory permissions are not private: " + directory);
        }
    }

    private static void validateSocketPathLength(Path udsParent) {
        Path longest = udsParent.resolve("runtime-pool-xxxxxxxxxxxx")
                .resolve("worker-64-g2147483647.sock");
        int bytes = longest.toString().getBytes(StandardCharsets.UTF_8).length;
        if (bytes > 100) {
            throw new IllegalArgumentException(
                    "Managed Python UDS path may exceed the 100-byte safe limit: " + longest);
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

    private void recordFailure(
            ManagedPythonFailureCategory category,
            ManagedPythonWorker worker,
            String context,
            Throwable failure) {
        String message = context;
        if (failure != null && failure.getMessage() != null && !failure.getMessage().isBlank()) {
            message += ": " + failure.getMessage();
        }
        message = boundedMessage(message);
        lastFailure.set(new ManagedPythonRuntimeSnapshot.FailureSummary(
                category.name(),
                worker == null ? "" : worker.workerId(),
                worker == null ? 0 : worker.generation(),
                worker == null ? -1 : worker.pid(),
                message,
                System.currentTimeMillis()));
    }

    private static String boundedMessage(Throwable failure) {
        return boundedMessage(failure == null ? "none" : failure.getMessage());
    }

    private static String boundedMessage(String message) {
        if (message == null || message.isBlank()) return "none";
        String sanitized = message.replace('\n', ' ').replace('\r', ' ');
        return sanitized.length() <= MAX_FAILURE_MESSAGE
                ? sanitized : sanitized.substring(0, MAX_FAILURE_MESSAGE);
    }

    private static final class RuntimeMetrics {
        private final LongAdder requestsAdmitted = new LongAdder();
        private final LongAdder requestsCompleted = new LongAdder();
        private final LongAdder requestsFailed = new LongAdder();
        private final LongAdder queueFullObservations = new LongAdder();
        private final LongAdder queueTimeouts = new LongAdder();
        private final LongAdder preTransmissionTimeouts = new LongAdder();
        private final LongAdder postTransmissionTimeouts = new LongAdder();
        private final LongAdder workerCrashes = new LongAdder();
        private final LongAdder workerRestarts = new LongAdder();
        private final LongAdder restartExhaustions = new LongAdder();
        private final LongAdder healthChecksSent = new LongAdder();
        private final LongAdder healthChecksSucceeded = new LongAdder();
        private final LongAdder healthChecksFailed = new LongAdder();
        private final LongAdder forcedTerminations = new LongAdder();
        private final LongAdder cleanupFailures = new LongAdder();
        private final MetricTimer queueWait = new MetricTimer();
        private final MetricTimer requestExecution = new MetricTimer();
        private final MetricTimer workerStartup = new MetricTimer();
        private final MetricTimer workerReplacement = new MetricTimer();
        private final MetricTimer poolShutdown = new MetricTimer();

        private ManagedPythonRuntimeSnapshot.Metrics snapshot() {
            return new ManagedPythonRuntimeSnapshot.Metrics(
                    requestsAdmitted.sum(), requestsCompleted.sum(), requestsFailed.sum(),
                    queueFullObservations.sum(), queueTimeouts.sum(),
                    preTransmissionTimeouts.sum(), postTransmissionTimeouts.sum(),
                    workerCrashes.sum(), workerRestarts.sum(), restartExhaustions.sum(),
                    healthChecksSent.sum(), healthChecksSucceeded.sum(), healthChecksFailed.sum(),
                    forcedTerminations.sum(), cleanupFailures.sum(),
                    queueWait.snapshot(), requestExecution.snapshot(), workerStartup.snapshot(),
                    workerReplacement.snapshot(), poolShutdown.snapshot());
        }
    }

    private static final class MetricTimer {
        private final LongAdder count = new LongAdder();
        private final LongAdder totalNanos = new LongAdder();
        private final AtomicLong maximumNanos = new AtomicLong();

        private void record(long elapsedNanos) {
            long value = Math.max(0, elapsedNanos);
            count.increment();
            totalNanos.add(value);
            maximumNanos.accumulateAndGet(value, Math::max);
        }

        private ManagedPythonRuntimeSnapshot.Timer snapshot() {
            return new ManagedPythonRuntimeSnapshot.Timer(
                    count.sum(), totalNanos.sum(), maximumNanos.get());
        }
    }

    private static final class Submission {
        private final PythonCallRequest request;
        private final long admittedAtNs;
        private final long queueDeadlineNs;
        private final CompletableFuture<PythonCallResponse> completion = new CompletableFuture<>();
        private final AtomicReference<ManagedPythonWorker> assignedWorker = new AtomicReference<>();
        private volatile ManagedPythonWorker.Assignment assignment;

        private Submission(PythonCallRequest request, long admittedAtNs, long queueDeadlineNs) {
            this.request = request;
            this.admittedAtNs = admittedAtNs;
            this.queueDeadlineNs = queueDeadlineNs;
        }
    }

    private final class WorkerSlot implements Runnable {
        private final String workerId;
        private final Deque<Long> restartAttempts = new ArrayDeque<>();
        private final Thread supervisor;
        private ManagedPythonWorker currentWorker;
        private int generation;
        private boolean currentHealthy;
        private boolean exhausted;
        private boolean restarting;

        private WorkerSlot(String workerId) {
            this.workerId = workerId;
            this.supervisor = new Thread(this, "managed-python-supervisor-" + workerId);
        }

        private void start() {
            supervisor.start();
        }

        @Override
        public void run() {
            boolean initialAttempt = true;
            try {
                while (!closeStarted.get()) {
                    if (!initialAttempt) {
                        synchronized (this) {
                            restarting = true;
                        }
                        long backoff = reserveRestartBackoff();
                        if (backoff < 0) {
                            markExhausted();
                            return;
                        }
                        if (backoff > 0) Thread.sleep(backoff);
                        if (closeStarted.get()) return;
                    }

                    int nextGeneration;
                    synchronized (this) {
                        nextGeneration = ++generation;
                    }
                    ManagedPythonWorker worker;
                    long attemptStarted = System.nanoTime();
                    try {
                        long attemptDeadline = initialAttempt
                                ? initialStartupDeadlineNs
                                : System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.startupTimeoutMs());
                        LOGGER.info("event=managed_python_worker_starting workerId={} generation={} replacement={}",
                                workerId, nextGeneration, !initialAttempt);
                        worker = ManagedPythonWorker.start(
                                config, runtimeDirectory, workerId, nextGeneration,
                                ManagedPythonRuntime.this, attemptDeadline);
                        metrics.workerStartup.record(System.nanoTime() - attemptStarted);
                        if (!initialAttempt) {
                            metrics.workerRestarts.increment();
                            metrics.workerReplacement.record(System.nanoTime() - attemptStarted);
                        }
                        boolean lateDuringShutdown;
                        synchronized (dispatchStateLock) {
                            lateDuringShutdown = closeStarted.get();
                            if (!lateDuringShutdown) {
                                synchronized (this) {
                                    currentWorker = worker;
                                    currentHealthy = true;
                                    restarting = false;
                                    exhausted = false;
                                }
                                healthyWorkers.incrementAndGet();
                                publishAvailable(worker);
                            }
                        }
                        if (lateDuringShutdown) {
                            worker.forceStop(shutdownFailure());
                            return;
                        }
                        updatePoolState();
                    } catch (Exception startupFailure) {
                        metrics.workerStartup.record(System.nanoTime() - attemptStarted);
                        ManagedPythonFailureCategory startupCategory =
                                startupFailure instanceof ManagedPythonWorker.StartupException categorized
                                        ? categorized.category()
                                        : startupFailure instanceof TimeoutException
                                        ? ManagedPythonFailureCategory.STARTUP_TIMEOUT
                                        : ManagedPythonFailureCategory.STARTUP_FAILURE;
                        recordFailure(startupCategory,
                                null, "Worker startup failed for " + workerId + " generation " + nextGeneration,
                                startupFailure);
                        LOGGER.warn("event=managed_python_worker_startup_failed workerId={} generation={} "
                                        + "failureCategory={} failure={}",
                                workerId, nextGeneration, startupCategory,
                                boundedMessage(startupFailure));
                        if (initialAttempt) firstAttempts.countDown();
                        initialAttempt = false;
                        initialDecision.await();
                        continue;
                    }

                    if (initialAttempt) firstAttempts.countDown();
                    initialAttempt = false;
                    initialDecision.await();
                    worker.awaitProcessExit();
                    worker.processExited();
                    synchronized (this) {
                        if (currentWorker == worker) {
                            if (currentHealthy) {
                                currentHealthy = false;
                                healthyWorkers.decrementAndGet();
                            }
                            currentWorker = null;
                        }
                    }
                    availableWorkers.remove(worker);
                    updatePoolState();
                }
            } catch (InterruptedException interrupted) {
                if (!closeStarted.get()) {
                    Thread.currentThread().interrupt();
                    markExhausted();
                }
            }
        }

        private synchronized long reserveRestartBackoff() {
            if (!config.restartEnabled()) return -1;
            long now = System.nanoTime();
            long window = TimeUnit.MILLISECONDS.toNanos(config.restartWindowMs());
            while (!restartAttempts.isEmpty() && now - restartAttempts.peekFirst() >= window) {
                restartAttempts.removeFirst();
            }
            if (restartAttempts.size() >= config.restartMaxAttempts()) return -1;
            int attemptNumber = restartAttempts.size() + 1;
            restartAttempts.addLast(now);
            long multiplier = 1L << Math.min(attemptNumber - 1, 30);
            long calculated;
            try {
                calculated = Math.multiplyExact(config.restartInitialBackoffMs(), multiplier);
            } catch (ArithmeticException overflow) {
                calculated = Long.MAX_VALUE;
            }
            long backoff = Math.min(calculated, config.restartMaximumBackoffMs());
            LOGGER.warn("event=managed_python_worker_restart_scheduled workerId={} attempt={} backoffMs={}",
                    workerId, attemptNumber, backoff);
            return backoff;
        }

        private synchronized boolean markUnhealthy(ManagedPythonWorker worker) {
            if (currentWorker != worker || !currentHealthy) return false;
            currentHealthy = false;
            return true;
        }

        private synchronized boolean isCurrent(ManagedPythonWorker worker) {
            return currentWorker == worker && currentHealthy;
        }

        private synchronized ManagedPythonWorker currentWorker() {
            return currentWorker;
        }

        private synchronized boolean isExhausted() {
            return exhausted;
        }

        private synchronized SlotSnapshot slotSnapshot() {
            return new SlotSnapshot(currentWorker, generation, restarting, exhausted);
        }

        private void markExhausted() {
            synchronized (this) {
                exhausted = true;
                restarting = false;
            }
            metrics.restartExhaustions.increment();
            recordFailure(ManagedPythonFailureCategory.RESTART_EXHAUSTED, null,
                    "Worker restart budget exhausted for " + workerId, null);
            updatePoolState();
            if (healthyWorkers.get() == 0 && allSlotsExhausted()) {
                Submission queued;
                while ((queued = admissionQueue.poll()) != null) {
                    queued.completion.completeExceptionally(unavailableFailure());
                }
                Submission owned = dispatcherOwned.get();
                if (owned != null) owned.completion.completeExceptionally(unavailableFailure());
            }
            LOGGER.error("event=managed_python_worker_restart_exhausted workerId={}", workerId);
        }

        private void interruptSupervisor() {
            supervisor.interrupt();
        }

        private void joinSupervisor(long deadlineNs) {
            try {
                long remaining = remainingNanos(deadlineNs);
                if (remaining > 0) TimeUnit.NANOSECONDS.timedJoin(supervisor, remaining);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
            }
        }

        private record SlotSnapshot(
                ManagedPythonWorker worker,
                int generation,
                boolean restarting,
                boolean exhausted) {
        }
    }
}
