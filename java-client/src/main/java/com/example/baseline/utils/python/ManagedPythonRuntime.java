package com.example.baseline.utils.python;

import com.example.baseline.utils.config.ApplicationConfig.ManagedPythonRuntimeConfig;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

final class ManagedPythonRuntime implements PythonCallExecutor, ManagedPythonWorker.Listener {
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
    private final Object stateLock = new Object();
    private final Object dispatchStateLock = new Object();
    private volatile PoolState state = PoolState.STARTING;
    private Thread dispatcherThread;

    private ManagedPythonRuntime(ManagedPythonRuntimeConfig config, Path runtimeDirectory) {
        this.config = config;
        this.runtimeDirectory = runtimeDirectory;
        this.admissionQueue = new ArrayBlockingQueue<>(config.queueCapacity(), true);
        this.availableWorkers = new ArrayBlockingQueue<>(config.workerCount(), true);
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
        if (!Files.isDirectory(applicationDirectory)) {
            throw new IllegalArgumentException(
                    "Managed Python application directory does not exist: " + applicationDirectory);
        }
        validateExecutable(config.pythonExecutable());

        Path udsParent = Path.of(config.udsDirectory());
        Files.createDirectories(udsParent);
        Path runtimeDirectory = Files.createTempDirectory(udsParent, "runtime-pool-");
        setPrivatePermissionsWhenSupported(runtimeDirectory);
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
        for (WorkerSlot slot : slots) slot.start();
        long startupDeadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.startupTimeoutMs() + 1000);
        if (!firstAttempts.await(remainingNanos(startupDeadline), TimeUnit.NANOSECONDS)) {
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
        initialDecision.countDown();
        /* 
        System.out.printf("Managed Python Runtime pool ready: state=%s, readyWorkers=%d, configuredWorkers=%d, directory=%s%n",
                state, ready, config.workerCount(), runtimeDirectory);
                */
    }

    @Override
    public PythonCallResponse call(PythonCallRequest request) throws Exception {
        if (!accepting.get()) throw unavailableOrShutdownFailure();
        if (healthyWorkers.get() == 0 && allSlotsExhausted()) throw unavailableFailure();
        long admittedAt = System.nanoTime();
        long queueDeadline = admittedAt + TimeUnit.MILLISECONDS.toNanos(config.queueTimeoutMs());
        long callerDeadline = queueDeadline + TimeUnit.MILLISECONDS.toNanos(config.requestTimeoutMs());
        Submission submission = new Submission(request, queueDeadline);
        long remaining = remainingNanos(queueDeadline);
        if (remaining <= 0 || !admissionQueue.offer(submission, remaining, TimeUnit.NANOSECONDS)) {
            throw new TimeoutException("Managed Python Runtime admission queue timed out");
        }
        if (!accepting.get() && admissionQueue.remove(submission)) {
            submission.completion.completeExceptionally(shutdownFailure());
        }
        try {
            remaining = remainingNanos(callerDeadline);
            if (remaining <= 0) throw new TimeoutException("Managed Python Runtime request timed out");
            return submission.completion.get(remaining, TimeUnit.NANOSECONDS);
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
                    return submission.completion.get();
                } catch (ExecutionException completedFailure) {
                    Throwable cause = completedFailure.getCause();
                    if (cause instanceof Exception checked) throw checked;
                    throw new IllegalStateException("Managed Python Runtime request failed", cause);
                }
            }
            throw timeout;
        } catch (ExecutionException failure) {
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
    public void onUnhealthy(ManagedPythonWorker worker, Exception failure) {
        worker.clearAvailabilityPublication();
        availableWorkers.remove(worker);
        WorkerSlot slot = slotFor(worker.workerId());
        if (slot.markUnhealthy(worker)) {
            healthyWorkers.decrementAndGet();
            updatePoolState();
        }
    }

    @Override
    public void close() {
        if (!closeStarted.compareAndSet(false, true)) return;
        synchronized (dispatchStateLock) {
            accepting.set(false);
        }
        synchronized (stateLock) {
            state = PoolState.DRAINING;
        }
        initialDecision.countDown();
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.shutdownTimeoutMs());
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
        for (ManagedPythonWorker worker : workers) if (!worker.isStopped()) worker.forceStop(timeout);
        for (WorkerSlot slot : slots) slot.interruptSupervisor();
        for (WorkerSlot slot : slots) slot.joinSupervisor(deadline);
        deleteRuntimeDirectory();
        synchronized (stateLock) {
            state = PoolState.STOPPED;
        }
        /*
        System.out.printf("Managed Python Runtime pool stopped: workers=%d, directoryRemoved=%s%n",
                workers.size(), !Files.exists(runtimeDirectory));
                */
    }

    private void abortStartup() {
        closeStarted.set(true);
        accepting.set(false);
        synchronized (stateLock) {
            state = PoolState.DRAINING;
        }
        initialDecision.countDown();
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.shutdownTimeoutMs());
        for (ManagedPythonWorker worker : currentWorkers()) worker.forceStop(shutdownFailure());
        for (WorkerSlot slot : slots) slot.interruptSupervisor();
        for (WorkerSlot slot : slots) slot.joinSupervisor(deadline);
        deleteRuntimeDirectory();
        synchronized (stateLock) {
            state = PoolState.STOPPED;
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
            /*
            System.out.printf("Managed Python Runtime pool state: previous=%s, current=%s, healthyWorkers=%d%n",
                    previous, next, healthy);
                    */
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
            if (Files.exists(runtimeDirectory)) {
                try (var paths = Files.list(runtimeDirectory)) {
                    paths.forEach(path -> {
                        try {
                            Files.deleteIfExists(path);
                        } catch (IOException failure) {
                            System.err.printf("Unable to remove managed runtime path %s: %s%n",
                                    path, failure.getMessage());
                        }
                    });
                }
            }
            Files.deleteIfExists(runtimeDirectory);
        } catch (IOException failure) {
            System.err.printf("Unable to remove managed runtime directory %s: %s%n",
                    runtimeDirectory, failure.getMessage());
        }
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
        private final long queueDeadlineNs;
        private final CompletableFuture<PythonCallResponse> completion = new CompletableFuture<>();
        private final AtomicReference<ManagedPythonWorker> assignedWorker = new AtomicReference<>();
        private volatile ManagedPythonWorker.Assignment assignment;

        private Submission(PythonCallRequest request, long queueDeadlineNs) {
            this.request = request;
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
                    try {
                        worker = ManagedPythonWorker.start(
                                config, runtimeDirectory, workerId, nextGeneration, ManagedPythonRuntime.this);
                        boolean lateDuringShutdown;
                        synchronized (dispatchStateLock) {
                            lateDuringShutdown = closeStarted.get();
                            if (!lateDuringShutdown) {
                                synchronized (this) {
                                    currentWorker = worker;
                                    currentHealthy = true;
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
                        System.err.printf("Managed Python worker startup failed: workerId=%s, generation=%d, failure=%s%n",
                                workerId, nextGeneration, startupFailure.getMessage());
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
            /* 
            System.out.printf("Managed Python worker restart scheduled: workerId=%s, attempt=%d, backoffMs=%d%n",
                    workerId, attemptNumber, backoff);
                    */
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

        private void markExhausted() {
            synchronized (this) {
                exhausted = true;
            }
            updatePoolState();
            if (healthyWorkers.get() == 0 && allSlotsExhausted()) {
                Submission queued;
                while ((queued = admissionQueue.poll()) != null) {
                    queued.completion.completeExceptionally(unavailableFailure());
                }
                Submission owned = dispatcherOwned.get();
                if (owned != null) owned.completion.completeExceptionally(unavailableFailure());
            }
            System.err.printf("Managed Python worker slot exhausted: workerId=%s%n", workerId);
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
    }
}
