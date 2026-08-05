package com.example.baseline.e2e;

import com.example.baseline.client.RestProcessingClient;
import com.example.baseline.dto.CallResult;
import com.example.baseline.dto.ProcessRequest;
import com.example.baseline.utils.config.ApplicationConfig;
import com.example.baseline.utils.config.ApplicationConfig.ManagedPythonRuntimeConfig;
import com.example.baseline.utils.config.YamlConfigLoader;
import com.example.baseline.utils.python.PythonCallMode;
import com.example.baseline.utils.python.PythonCallUtil;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;

public final class Phase2E2ERunner {
    private Phase2E2ERunner() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException("Usage: Phase2E2ERunner <SCENARIO>");
        }
        ApplicationConfig base = YamlConfigLoader.load();
        switch (args[0]) {
            case "PARALLEL" -> parallel(base);
            case "QUEUE" -> queue(base);
            case "IDLE_FAILURE" -> idleFailure(base);
            case "BUSY_FAILURE" -> busyFailure(base);
            case "STARTUP_FAILURE" -> startupFailure(base);
            case "RESTART_EXHAUSTION" -> restartExhaustion(base);
            case "REQUEST_TIMEOUT" -> requestTimeout(base);
            case "SHUTDOWN" -> shutdown(base);
            case "SHUTDOWN_TIMEOUT" -> shutdownTimeout(base);
            default -> throw new IllegalArgumentException("Unsupported Phase 2 E2E scenario: " + args[0]);
        }
    }

    private static void parallel(ApplicationConfig config) throws Exception {
        ExecutorService callers = Executors.newFixedThreadPool(8);
        try {
            PythonCallUtil.initialize(PythonCallMode.MANAGED_RUNTIME, config);
            Set<Long> initialPids = waitForWorkerCount(4, 10_000);
            RestProcessingClient client = new RestProcessingClient(config.fastApi());
            long startNs = System.nanoTime();
            List<CompletableFuture<CallResult>> futures = calls(client, callers, 8, 500, "parallel");
            List<CallResult> results = successfulResults(futures, 8_000);
            double wallMs = (System.nanoTime() - startNs) / 1_000_000.0;
            int maximumOverlap = maximumPythonOverlap(results);
            Set<Long> reusedPids = waitForWorkerCount(4, 2_000);
            require(initialPids.equals(reusedPids), "The worker PID set changed during normal requests");
            require(maximumOverlap >= 4, "Expected at least four overlapping Python executions, observed " + maximumOverlap);
            require(wallMs < 3_000, "Four-worker workload was not materially below serial execution: " + wallMs);
            System.out.printf("Phase 2 PARALLEL passed: workerPids=%s, reusedPids=%s, requests=8, "
                            + "maximumOverlap=%d, wallMs=%.3f%n",
                    initialPids, reusedPids, maximumOverlap, wallMs);
        } finally {
            shutdownAndVerify(config, callers);
        }
    }

    private static void queue(ApplicationConfig base) throws Exception {
        ApplicationConfig config = withManaged(base, managed(base, 1, 2, 300, 3_000, 2_000, true, 2));
        ExecutorService callers = Executors.newFixedThreadPool(6);
        try {
            PythonCallUtil.initialize(PythonCallMode.MANAGED_RUNTIME, config);
            waitForWorkerCount(1, 5_000);
            RestProcessingClient client = new RestProcessingClient(config.fastApi());
            long start = System.nanoTime();
            List<CompletableFuture<CallResult>> futures = calls(client, callers, 6, 1_000, "queue");
            List<Outcome> outcomes = outcomes(futures, 5_000);
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            long failures = outcomes.stream().filter(outcome -> outcome.failure != null).count();
            require(failures >= 2, "Expected bounded queue failures, observed " + failures);
            require(outcomes.stream().allMatch(Outcome::terminal), "A queue scenario future was not terminal");
            require(outcomes.stream().filter(outcome -> outcome.failure != null)
                            .allMatch(outcome -> messageChain(outcome.failure).contains("timed out")),
                    "A queue failure did not report its timeout bound");
            System.out.printf("Phase 2 QUEUE passed: workerCount=1, queueCapacity=2, calls=6, "
                    + "failures=%d, allFuturesTerminal=true, elapsedMs=%d%n", failures, elapsedMs);
        } finally {
            shutdownAndVerify(config, callers);
        }
    }

    private static void idleFailure(ApplicationConfig config) throws Exception {
        ExecutorService caller = Executors.newFixedThreadPool(4);
        try {
            PythonCallUtil.initialize(PythonCallMode.MANAGED_RUNTIME, config);
            Set<Long> before = waitForWorkerCount(4, 10_000);
            long killed = before.iterator().next();
            kill(killed);
            waitUntil(() -> !ProcessHandle.of(killed).map(ProcessHandle::isAlive).orElse(false), 3_000,
                    "Killed idle worker remained alive");
            Set<Long> after = waitForChangedPool(before, 4, 10_000);
            Thread.sleep(500);
            RestProcessingClient client = new RestProcessingClient(config.fastApi());
            List<CallResult> replacementResults = successfulResults(
                    calls(client, caller, 4, 500, "idle-replacement"), 5_000);
            require(maximumPythonOverlap(replacementResults) == 4,
                    "Replacement was not READY and serving as restored fourth-worker capacity");
            require(!after.contains(killed), "Killed PID remained in the replacement pool");
            System.out.printf("Phase 2 IDLE_FAILURE passed: killedPid=%d, before=%s, after=%s, "
                            + "restoredParallelCapacity=4%n", killed, before, after);
        } finally {
            shutdownAndVerify(config, caller);
        }
    }

    private static void busyFailure(ApplicationConfig config) throws Exception {
        ExecutorService callers = Executors.newFixedThreadPool(4);
        try {
            PythonCallUtil.initialize(PythonCallMode.MANAGED_RUNTIME, config);
            Set<Long> before = waitForWorkerCount(4, 10_000);
            RestProcessingClient client = new RestProcessingClient(config.fastApi());
            List<CompletableFuture<CallResult>> futures = calls(client, callers, 4, 1_500, "busy-kill");
            Thread.sleep(300);
            long killed = before.iterator().next();
            kill(killed);
            List<Outcome> outcomes = outcomes(futures, 6_000);
            long succeeded = outcomes.stream().filter(outcome -> outcome.result != null).count();
            long failed = outcomes.stream().filter(outcome -> outcome.failure != null).count();
            require(succeeded == 3 && failed == 1,
                    "Expected one non-retried busy failure and three successes; successes=" + succeeded + ", failures=" + failed);
            Set<Long> after = waitForChangedPool(before, 4, 10_000);
            CallResult subsequent = client.process(new ProcessRequest("busy-replacement", "usable", 10));
            require(subsequent.httpStatus() == 200, "Pool was not usable after busy worker replacement");
            System.out.printf("Phase 2 BUSY_FAILURE passed: killedPid=%d, activeSucceeded=%d, "
                            + "activeFailedWithoutRetry=%d, replacementPids=%s%n",
                    killed, succeeded, failed, after);
        } finally {
            shutdownAndVerify(config, callers);
        }
    }

    private static void startupFailure(ApplicationConfig base) throws Exception {
        ManagedPythonRuntimeConfig original = base.managedPythonRuntime();
        ManagedPythonRuntimeConfig invalid = new ManagedPythonRuntimeConfig(
                "phase2-python-command-does-not-exist",
                original.applicationDirectory(), original.udsDirectory(), 2,
                original.queueCapacity(), 500, original.maxFrameBytes(), 750,
                original.requestTimeoutMs(), 1_000, false, 0, 0, 0, 1_000);
        ApplicationConfig config = withManaged(base, invalid);
        Set<Long> before = workerPids();
        long start = System.nanoTime();
        Throwable failure;
        try {
            PythonCallUtil.initialize(PythonCallMode.MANAGED_RUNTIME, config);
            throw new IllegalStateException("Invalid Python command unexpectedly initialized");
        } catch (IllegalStateException expected) {
            failure = expected;
        } finally {
            PythonCallUtil.close();
        }
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        require(!messageChain(failure).contains("unexpectedly initialized"),
                "Invalid Python command unexpectedly initialized");
        require(elapsedMs < 3_000, "Startup failure exceeded its bound: " + elapsedMs);
        require(workerPids().equals(before), "Startup failure leaked a Python child process");
        verifyNoRuntimeDirectories(config);
        System.out.printf("Phase 2 STARTUP_FAILURE passed: elapsedMs=%d, childPidsBefore=%s, "
                        + "childPidsAfter=%s, failure=%s%n",
                elapsedMs, before, workerPids(), messageChain(failure));
    }

    private static void restartExhaustion(ApplicationConfig base) throws Exception {
        ApplicationConfig config = withManaged(base, managed(base, 1, 4, 2_000, 2_000, 1_000, true, 2));
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try {
            PythonCallUtil.initialize(PythonCallMode.MANAGED_RUNTIME, config);
            Set<Long> seen = new HashSet<>();
            long current = waitForWorkerCount(1, 5_000).iterator().next();
            seen.add(current);
            RestProcessingClient client = new RestProcessingClient(config.fastApi());
            for (int killNumber = 0; killNumber < 3; killNumber++) {
                kill(current);
                if (killNumber < 2) {
                    current = waitForNewPid(seen, 8_000);
                    seen.add(current);
                    CallResult ready = client.process(new ProcessRequest(
                            "restart-ready-" + killNumber, "ready", 1));
                    require(ready.httpStatus() == 200, "Replacement generation was not READY");
                }
            }
            waitUntil(() -> workerPids().isEmpty(), 5_000, "Exhausted worker remained alive");
            Thread.sleep(500);
            long failureStart = System.nanoTime();
            Throwable unavailable = callFailure(client,
                    new ProcessRequest("exhausted", "unavailable", 1));
            long failureMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - failureStart);
            require(messageChain(unavailable).contains("no available worker capacity"),
                    "Exhausted pool did not report unavailable capacity: " + unavailable);
            require(failureMs < 1_000, "Unavailable call was not rejected promptly: " + failureMs);
            System.out.printf("Phase 2 RESTART_EXHAUSTION passed: generations=%s, restarts=2, "
                    + "unavailableFailureMs=%d, noInfiniteRestart=true%n", seen, failureMs);
        } finally {
            shutdownAndVerify(config, caller);
        }
    }

    private static void requestTimeout(ApplicationConfig base) throws Exception {
        ApplicationConfig config = withManaged(base, managed(base, 1, 4, 2_000, 300, 1_000, true, 2));
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try {
            PythonCallUtil.initialize(PythonCallMode.MANAGED_RUNTIME, config);
            Set<Long> before = waitForWorkerCount(1, 5_000);
            Throwable timeout = callFailure(new RestProcessingClient(config.fastApi()),
                    new ProcessRequest("request-timeout", "timeout", 1_000));
            require(messageChain(timeout).contains("timed out"), "Request timeout was not deterministic: " + timeout);
            Set<Long> after = waitForChangedPool(before, 1, 8_000);
            CallResult result = new RestProcessingClient(config.fastApi()).process(
                    new ProcessRequest("after-timeout", "replacement", 10));
            require(result.httpStatus() == 200, "Replacement was not usable after request timeout");
            System.out.printf("Phase 2 REQUEST_TIMEOUT passed: poisonedPid=%s, replacementPid=%s, "
                    + "timedOutRequestNotRetried=true%n", before, after);
        } finally {
            shutdownAndVerify(config, caller);
        }
    }

    private static void shutdown(ApplicationConfig config) throws Exception {
        ExecutorService callers = Executors.newFixedThreadPool(10);
        Set<Long> pids = Set.of();
        try {
            PythonCallUtil.initialize(PythonCallMode.MANAGED_RUNTIME, config);
            pids = waitForWorkerCount(4, 10_000);
            RestProcessingClient client = new RestProcessingClient(config.fastApi());
            List<CompletableFuture<CallResult>> active = calls(client, callers, 4, 1_000, "shutdown-active");
            Thread.sleep(250);
            List<CompletableFuture<CallResult>> queued = calls(client, callers, 4, 100, "shutdown-queued");
            Thread.sleep(100);
            long start = System.nanoTime();
            CompletableFuture<Void> closing = CompletableFuture.runAsync(PythonCallUtil::close, callers);
            Thread.sleep(50);
            Throwable rejected = callFailure(client, new ProcessRequest("shutdown-new", "rejected", 1));
            List<CallResult> activeResults = successfulResults(active, 5_000);
            List<Outcome> queuedOutcomes = outcomes(queued, 5_000);
            closing.get(6, TimeUnit.SECONDS);
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            require(activeResults.size() == 4, "Not all active workers drained successfully");
            require(queuedOutcomes.stream().allMatch(outcome -> outcome.failure != null),
                    "A queued shutdown request was executed");
            require(messageChain(rejected).contains("SHUTTING_DOWN") || messageChain(rejected).contains("CLOSED"),
                    "New call was not rejected during shutdown");
            verifyPidsGone(pids, 3_000);
            verifyNoRuntimeDirectories(config);
            System.out.printf("Phase 2 SHUTDOWN passed: drainedActive=4, failedQueued=4, "
                    + "newCallsRejected=true, allFuturesTerminal=true, shutdownMs=%d, removedPids=%s%n",
                    elapsedMs, pids);
        } finally {
            PythonCallUtil.close();
            callers.shutdownNow();
            callers.awaitTermination(5, TimeUnit.SECONDS);
            verifyPidsGone(pids, 3_000);
            verifyNoRuntimeDirectories(config);
        }
    }

    private static void shutdownTimeout(ApplicationConfig base) throws Exception {
        ApplicationConfig config = withManaged(base, managed(base, 4, 8, 1_000, 5_000, 500, true, 2));
        ExecutorService callers = Executors.newFixedThreadPool(8);
        Set<Long> pids = Set.of();
        try {
            PythonCallUtil.initialize(PythonCallMode.MANAGED_RUNTIME, config);
            pids = waitForWorkerCount(4, 10_000);
            RestProcessingClient client = new RestProcessingClient(config.fastApi());
            List<CompletableFuture<CallResult>> active = calls(client, callers, 4, 2_000, "shutdown-timeout");
            Thread.sleep(250);
            long start = System.nanoTime();
            PythonCallUtil.close();
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            List<Outcome> outcomes = outcomes(active, 2_000);
            require(outcomes.stream().allMatch(outcome -> outcome.failure != null),
                    "An over-time active request did not fail");
            require(outcomes.stream().allMatch(Outcome::terminal), "An active caller future leaked");
            require(elapsedMs < 1_500, "Pool shutdown multiplied the timeout by worker count: " + elapsedMs);
            require(outcomes.stream().allMatch(outcome ->
                            messageChain(outcome.failure).contains("common shutdown timeout")),
                    "An active caller did not receive the deterministic pool timeout");
            verifyPidsGone(pids, 3_000);
            verifyNoRuntimeDirectories(config);
            System.out.printf("Phase 2 SHUTDOWN_TIMEOUT passed: failedActive=4, allFuturesTerminal=true, "
                    + "commonDeadlineMs=500, shutdownMs=%d, removedPids=%s%n", elapsedMs, pids);
        } finally {
            PythonCallUtil.close();
            callers.shutdownNow();
            callers.awaitTermination(5, TimeUnit.SECONDS);
            verifyPidsGone(pids, 3_000);
            verifyNoRuntimeDirectories(config);
        }
    }

    private static List<CompletableFuture<CallResult>> calls(
            RestProcessingClient client, ExecutorService callers, int count, int delayMs, String prefix) {
        List<CompletableFuture<CallResult>> futures = new ArrayList<>();
        for (int index = 0; index < count; index++) {
            int requestNumber = index;
            futures.add(CompletableFuture.supplyAsync(() -> {
                try {
                    return client.process(new ProcessRequest(
                            prefix + '-' + requestNumber, "message-" + requestNumber, delayMs));
                } catch (Exception failure) {
                    throw new CompletionException(failure);
                }
            }, callers));
        }
        return futures;
    }

    private static List<CallResult> successfulResults(
            List<CompletableFuture<CallResult>> futures, long timeoutMs) throws Exception {
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).get(timeoutMs, TimeUnit.MILLISECONDS);
        return futures.stream().map(CompletableFuture::join).toList();
    }

    private static List<Outcome> outcomes(
            List<CompletableFuture<CallResult>> futures, long timeoutMs) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        List<Outcome> outcomes = new ArrayList<>();
        for (CompletableFuture<CallResult> future : futures) {
            try {
                outcomes.add(new Outcome(future.get(Math.max(1, remainingMillis(deadline)), TimeUnit.MILLISECONDS), null));
            } catch (ExecutionException failure) {
                outcomes.add(new Outcome(null, failure.getCause()));
            }
        }
        return outcomes;
    }

    private static Throwable callFailure(RestProcessingClient client, ProcessRequest request) {
        try {
            client.process(request);
            throw new IllegalStateException("Expected request to fail");
        } catch (Exception expected) {
            return expected;
        }
    }

    private static int maximumPythonOverlap(List<CallResult> results) {
        List<Instant> starts = results.stream().map(result -> Instant.parse(result.response().pythonStartTime())).toList();
        List<Instant> ends = results.stream().map(result -> Instant.parse(result.response().pythonEndTime())).toList();
        int maximum = 0;
        for (Instant instant : starts) {
            int overlap = 0;
            for (int index = 0; index < starts.size(); index++) {
                if (!starts.get(index).isAfter(instant) && !ends.get(index).isBefore(instant)) overlap++;
            }
            maximum = Math.max(maximum, overlap);
        }
        return maximum;
    }

    private static ManagedPythonRuntimeConfig managed(
            ApplicationConfig base,
            int workers,
            int queueCapacity,
            long queueTimeoutMs,
            long requestTimeoutMs,
            long shutdownTimeoutMs,
            boolean restartEnabled,
            int restartAttempts) {
        ManagedPythonRuntimeConfig value = base.managedPythonRuntime();
        return new ManagedPythonRuntimeConfig(
                value.pythonExecutable(), value.applicationDirectory(), value.udsDirectory(), workers,
                queueCapacity, queueTimeoutMs, value.maxFrameBytes(), value.startupTimeoutMs(),
                requestTimeoutMs, shutdownTimeoutMs, restartEnabled, 50, 200,
                restartAttempts, 5_000);
    }

    private static ApplicationConfig withManaged(
            ApplicationConfig base, ManagedPythonRuntimeConfig managed) {
        return new ApplicationConfig(base.fastApi(), base.workload(), base.httpClient(), managed);
    }

    private static Set<Long> workerPids() {
        Set<Long> pids = new HashSet<>();
        ProcessHandle.current().descendants()
                .filter(ProcessHandle::isAlive)
                .filter(handle -> handle.info().commandLine().orElse("").contains("python_runtime.worker_runtime"))
                .forEach(handle -> pids.add(handle.pid()));
        return Set.copyOf(pids);
    }

    private static Set<Long> waitForWorkerCount(int count, long timeoutMs) throws Exception {
        waitUntil(() -> workerPids().size() == count, timeoutMs,
                "Expected " + count + " Managed Python workers, observed " + workerPids());
        return workerPids();
    }

    private static Set<Long> waitForChangedPool(Set<Long> before, int count, long timeoutMs) throws Exception {
        waitUntil(() -> workerPids().size() == count && !workerPids().equals(before), timeoutMs,
                "Worker pool PID set did not change from " + before + "; observed " + workerPids());
        return workerPids();
    }

    private static long waitForNewPid(Set<Long> seen, long timeoutMs) throws Exception {
        waitUntil(() -> workerPids().stream().anyMatch(pid -> !seen.contains(pid)), timeoutMs,
                "A replacement worker did not appear; seen=" + seen + ", current=" + workerPids());
        return workerPids().stream().filter(pid -> !seen.contains(pid)).findFirst().orElseThrow();
    }

    private static void kill(long pid) {
        ProcessHandle handle = ProcessHandle.of(pid)
                .orElseThrow(() -> new IllegalStateException("Worker PID no longer exists: " + pid));
        require(handle.destroyForcibly(), "Unable to forcibly terminate worker PID " + pid);
    }

    private static void verifyPidsGone(Set<Long> pids, long timeoutMs) throws Exception {
        waitUntil(() -> pids.stream().noneMatch(pid ->
                        ProcessHandle.of(pid).map(ProcessHandle::isAlive).orElse(false)),
                timeoutMs, "Managed Python worker process remained alive: " + pids);
    }

    private static void shutdownAndVerify(ApplicationConfig config, ExecutorService executor) throws Exception {
        Set<Long> pids = workerPids();
        PythonCallUtil.close();
        executor.shutdownNow();
        executor.awaitTermination(5, TimeUnit.SECONDS);
        verifyPidsGone(pids, 3_000);
        verifyNoRuntimeDirectories(config);
    }

    private static void verifyNoRuntimeDirectories(ApplicationConfig config) throws Exception {
        Path parent = Path.of(config.managedPythonRuntime().udsDirectory()).toAbsolutePath().normalize();
        if (!Files.exists(parent)) return;
        try (var paths = Files.list(parent)) {
            require(paths.noneMatch(path -> path.getFileName().toString().startsWith("runtime-pool-")),
                    "Managed runtime directory was not removed from " + parent);
        }
    }

    private static void waitUntil(BooleanSupplier condition, long timeoutMs, String failureMessage) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() >= deadline) throw new TimeoutException(failureMessage);
            Thread.sleep(25);
        }
    }

    private static long remainingMillis(long deadlineNs) {
        return Math.max(1, TimeUnit.NANOSECONDS.toMillis(Math.max(0, deadlineNs - System.nanoTime())));
    }

    private static String messageChain(Throwable throwable) {
        StringBuilder result = new StringBuilder();
        for (Throwable current = throwable; current != null; current = current.getCause()) {
            if (current.getMessage() != null) result.append(current.getMessage()).append(' ');
        }
        return result.toString();
    }

    private static void require(boolean condition, String message) {
        if (!condition) throw new IllegalStateException(message);
    }

    private record Outcome(CallResult result, Throwable failure) {
        private boolean terminal() {
            return result != null || failure != null;
        }
    }
}
