package com.example.baseline.e2e;

import com.example.baseline.dto.ProcessResponse;
import com.example.baseline.utils.config.ApplicationConfig;
import com.example.baseline.utils.config.ApplicationConfig.ManagedPythonRuntimeConfig;
import com.example.baseline.utils.config.YamlConfigLoader;
import com.example.baseline.utils.python.ManagedPythonRuntimeSnapshot;
import com.example.baseline.utils.python.PythonCallMode;
import com.example.baseline.utils.python.PythonCallRequest;
import com.example.baseline.utils.python.PythonCallResponse;
import com.example.baseline.utils.python.PythonCallUtil;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

public final class Phase4E2ERunner {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private Phase4E2ERunner() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) throw new IllegalArgumentException("Usage: Phase4E2ERunner <SCENARIO>");
        ApplicationConfig base = YamlConfigLoader.load();
        switch (args[0]) {
            case "IDLE_HEALTH" -> idleHealth(base);
            case "STUCK_IDLE" -> stuckIdle(base);
            case "STALE_PONG" -> stalePong(base);
            case "BUSY_HEALTH_POLICY" -> busyHealthPolicy(base);
            case "DEGRADED_VISIBILITY" -> degradedVisibility(base);
            case "RESTART_EXHAUSTION_VISIBILITY" -> restartExhaustionVisibility(base);
            case "PARTIAL_STARTUP" -> partialStartup(base);
            case "HEALTH_SHUTDOWN_RACE" -> healthShutdownRace(base);
            case "SNAPSHOT_METRICS" -> snapshotMetrics(base);
            case "SOAK" -> soak(base);
            case "PARENT_TERMINATION" -> parentTerminationHold(base);
            case "RESOURCE_REPETITION" -> resourceRepetition(base);
            default -> throw new IllegalArgumentException("Unsupported Phase 4 E2E scenario: " + args[0]);
        }
    }

    private static void idleHealth(ApplicationConfig base) throws Exception {
        ApplicationConfig config = configured(base, 1, 2, 300, 100, 0, 3, 5_000);
        Set<Long> before = Set.of();
        try {
            PythonCallUtil.initialize(PythonCallMode.MANAGED_RUNTIME, config);
            before = waitForWorkerCount(1, 5_000);
            waitUntil(() -> snapshot().metrics().healthChecksSucceeded() >= 3, 4_000,
                    "Three idle health checks did not succeed");
            ManagedPythonRuntimeSnapshot health = snapshot();
            require(before.equals(workerPids()), "Worker PID changed during successful idle health checks");
            require(health.totalInFlight() == 0 && health.availableCapacity() == 2,
                    "Health checks consumed business capacity: " + health);
            ProcessResponse response = request(config, "idle-health", 10);
            require("idle-health".equals(response.requestId()), "Later business request mapping failed");
            System.out.printf("Phase 4 IDLE_HEALTH passed: pid=%s, healthSent=%d, healthSucceeded=%d, "
                            + "inFlight=0, availableCapacity=2%n",
                    before, health.metrics().healthChecksSent(), health.metrics().healthChecksSucceeded());
        } finally {
            shutdownAndVerify(config, before);
        }
    }

    private static void stuckIdle(ApplicationConfig base) throws Exception {
        ApplicationConfig config = configured(base, 1, 1, 300, 100, 0, 3, 5_000);
        Set<Long> allPids = new HashSet<>();
        try {
            PythonCallUtil.initialize(PythonCallMode.MANAGED_RUNTIME, config);
            long oldPid = onlyPid(waitForWorkerCount(1, 5_000));
            allPids.add(oldPid);
            signal(oldPid, "STOP");
            waitUntil(() -> !workerPids().contains(oldPid), 5_000,
                    "Health timeout did not terminate the stopped worker");
            waitUntil(() -> snapshot().fullyReady() && snapshot().metrics().workerRestarts() >= 1,
                    6_000, "Health-timeout replacement did not become ready");
            Set<Long> replacement = workerPids();
            allPids.addAll(replacement);
            require(!replacement.contains(oldPid), "Replacement reused the stopped worker PID");
            waitUntil(() -> snapshot().metrics().healthChecksFailed() >= 1, 2_000,
                    "Health timeout metric did not increment");
            request(config, "after-stuck-idle", 10);
            System.out.printf("Phase 4 STUCK_IDLE passed: oldPid=%d, replacementPid=%s, "
                            + "healthFailures=%d, requestTimeoutNotRequired=true%n",
                    oldPid, replacement, snapshot().metrics().healthChecksFailed());
        } finally {
            shutdownAndVerify(config, allPids);
        }
    }

    private static void stalePong(ApplicationConfig base) throws Exception {
        try (PythonFixture fixture = PythonFixture.stalePong()) {
            ApplicationConfig config = withPythonExecutable(
                    configured(base, 1, 1, 300, 150, 0, 3, 5_000), fixture.wrapper().toString());
            Set<Long> pids = Set.of();
            try {
                PythonCallUtil.initialize(PythonCallMode.MANAGED_RUNTIME, config);
                pids = waitForWorkerCount(1, 5_000);
                waitUntil(() -> snapshot().metrics().healthChecksSucceeded() >= 1, 3_000,
                        "Generation-safe health response did not complete");
                ManagedPythonRuntimeSnapshot value = snapshot();
                require(value.responsiveWorkers() == 1 && value.availableCapacity() == 1,
                        "Stale PONG changed worker publication or capacity");
                request(config, "after-stale-pong", 10);
                System.out.printf("Phase 4 STALE_PONG passed: pid=%s, generationMatched=true, "
                                + "availableCapacity=%d%n", pids, value.availableCapacity());
            } finally {
                shutdownAndVerify(config, pids);
            }
        }
    }

    private static void busyHealthPolicy(ApplicationConfig base) throws Exception {
        ApplicationConfig config = configured(base, 1, 2, 300, 100, 0, 3, 5_000);
        ExecutorService caller = Executors.newSingleThreadExecutor();
        Set<Long> pids = Set.of();
        try {
            PythonCallUtil.initialize(PythonCallMode.MANAGED_RUNTIME, config);
            pids = waitForWorkerCount(1, 5_000);
            long healthBefore = snapshot().metrics().healthChecksSent();
            CompletableFuture<ProcessResponse> active = CompletableFuture.supplyAsync(() -> {
                try {
                    return request(config, "busy-health", 900);
                } catch (Exception failure) {
                    throw new CompletionException(failure);
                }
            }, caller);
            waitUntil(() -> snapshot().totalInFlight() == 1, 2_000, "Request never became active");
            Thread.sleep(500);
            require(snapshot().metrics().healthChecksSent() == healthBefore,
                    "A health check was sent while business work was active");
            require("busy-health".equals(active.get(3, TimeUnit.SECONDS).requestId()),
                    "Busy request failed during health policy test");
            waitUntil(() -> snapshot().metrics().healthChecksSent() > healthBefore, 2_000,
                    "Idle health checks did not resume after business completion");
            require(pids.equals(workerPids()), "Busy worker was falsely replaced");
            System.out.printf("Phase 4 BUSY_HEALTH_POLICY passed: pid=%s, pingDuringActive=false, "
                    + "healthResumed=true%n", pids);
        } finally {
            caller.shutdownNow();
            caller.awaitTermination(2, TimeUnit.SECONDS);
            shutdownAndVerify(config, pids);
        }
    }

    private static void degradedVisibility(ApplicationConfig base) throws Exception {
        ApplicationConfig config = configured(base, 2, 1, 2_000, 500, 0, 3, 5_000);
        Set<Long> allPids = new HashSet<>();
        try {
            PythonCallUtil.initialize(PythonCallMode.MANAGED_RUNTIME, config);
            Set<Long> initial = waitForWorkerCount(2, 5_000);
            allPids.addAll(initial);
            long killed = initial.iterator().next();
            kill(killed);
            waitUntil(() -> "DEGRADED".equals(snapshot().poolState()), 3_000,
                    "Pool did not expose DEGRADED while one slot recovered");
            request(config, "degraded-service", 10);
            waitUntil(() -> snapshot().fullyReady(), 6_000, "Pool did not return to full readiness");
            Set<Long> replacement = workerPids();
            allPids.addAll(replacement);
            require(!replacement.contains(killed) && snapshot().metrics().workerRestarts() >= 1,
                    "Replacement PID or restart metric was missing");
            System.out.printf("Phase 4 DEGRADED_VISIBILITY passed: killedPid=%d, currentPids=%s, "
                            + "workerRestarts=%d, serviceContinued=true%n",
                    killed, replacement, snapshot().metrics().workerRestarts());
        } finally {
            shutdownAndVerify(config, allPids);
        }
    }

    private static void restartExhaustionVisibility(ApplicationConfig base) throws Exception {
        ApplicationConfig config = configured(base, 1, 1, 2_000, 500, 0, 1, 30_000);
        Set<Long> allPids = new HashSet<>();
        try {
            PythonCallUtil.initialize(PythonCallMode.MANAGED_RUNTIME, config);
            long first = onlyPid(waitForWorkerCount(1, 5_000));
            allPids.add(first);
            kill(first);
            waitUntil(() -> snapshot().fullyReady() && snapshot().metrics().workerRestarts() >= 1
                            && workerPids().size() == 1 && !workerPids().contains(first), 6_000,
                    "First replacement did not become ready");
            long replacement = onlyPid(workerPids());
            allPids.add(replacement);
            kill(replacement);
            waitUntil(() -> "UNAVAILABLE".equals(snapshot().poolState())
                    && snapshot().exhaustedWorkers() == 1, 5_000,
                    "Exhausted slot did not become visibly unavailable");
            Throwable failure = callFailure(config, "after-exhaustion", 10);
            require(messageChain(failure).contains("no available worker"),
                    "Unavailable call did not fail clearly: " + failure);
            System.out.printf("Phase 4 RESTART_EXHAUSTION_VISIBILITY passed: pids=%s, "
                            + "restartExhaustions=%d, poolState=%s%n",
                    allPids, snapshot().metrics().restartExhaustions(), snapshot().poolState());
        } finally {
            shutdownAndVerify(config, allPids);
        }
    }

    private static void partialStartup(ApplicationConfig base) throws Exception {
        try (PythonFixture fixture = PythonFixture.failFirstStart()) {
            ApplicationConfig config = withPythonExecutable(
                    configured(base, 2, 1, 2_000, 500, 0, 3, 10_000), fixture.wrapper().toString());
            Set<Long> pids = new HashSet<>();
            try {
                PythonCallUtil.initialize(PythonCallMode.MANAGED_RUNTIME, config);
                require(snapshot().ready(), "Partial startup did not provide minimum ready capacity");
                waitUntil(() -> snapshot().fullyReady(), 7_000,
                        "Failed initial slot was not replaced within the restart policy");
                pids.addAll(workerPids());
                require(pids.size() == 2 && snapshot().metrics().workerRestarts() >= 1,
                        "Partial startup recovery evidence was incomplete");
                request(config, "after-partial-startup", 10);
                System.out.printf("Phase 4 PARTIAL_STARTUP passed: pids=%s, workerRestarts=%d, "
                                + "fullyReady=true%n", pids, snapshot().metrics().workerRestarts());
            } finally {
                shutdownAndVerify(config, pids);
            }
        }
    }

    private static void healthShutdownRace(ApplicationConfig base) throws Exception {
        try (PythonFixture fixture = PythonFixture.delayedPong()) {
            ApplicationConfig config = withPythonExecutable(
                    configured(base, 1, 1, 300, 200, 0, 3, 800), fixture.wrapper().toString());
            Set<Long> pids = Set.of();
            try {
                PythonCallUtil.initialize(PythonCallMode.MANAGED_RUNTIME, config);
                pids = waitForWorkerCount(1, 5_000);
                waitUntil(() -> snapshot().metrics().healthChecksSent() >= 1, 2_000,
                        "Delayed health check was not transmitted");
                long start = System.nanoTime();
                PythonCallUtil.close();
                long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                require(elapsedMs < 1_500, "Health/shutdown race exceeded its common bound: " + elapsedMs);
                verifyPidsGone(pids, 3_000);
                verifyNoRuntimeDirectories(config);
                require(noManagedThreads(), "Managed worker or health thread leaked after shutdown");
                System.out.printf("Phase 4 HEALTH_SHUTDOWN_RACE passed: pids=%s, shutdownMs=%d, "
                        + "managedThreadsRemaining=0%n", pids, elapsedMs);
            } finally {
                PythonCallUtil.close();
                verifyPidsGone(pids, 3_000);
                verifyNoRuntimeDirectories(config);
            }
        }
    }

    private static void snapshotMetrics(ApplicationConfig base) throws Exception {
        ApplicationConfig config = configured(base, 1, 1, 300, 100, 0, 3, 5_000);
        Set<Long> pids = new HashSet<>();
        try {
            PythonCallUtil.initialize(PythonCallMode.MANAGED_RUNTIME, config);
            pids.addAll(waitForWorkerCount(1, 5_000));
            request(config, "snapshot-success", 20);
            waitUntil(() -> snapshot().metrics().healthChecksSucceeded() >= 1, 2_000,
                    "Health metric was not populated");
            long killed = onlyPid(workerPids());
            kill(killed);
            waitUntil(() -> snapshot().fullyReady() && snapshot().metrics().workerRestarts() >= 1
                            && workerPids().size() == 1 && !workerPids().contains(killed), 6_000,
                    "Metrics scenario did not replace the worker");
            pids.addAll(workerPids());
            ManagedPythonRuntimeSnapshot value = snapshot();
            require(value.metrics().requestsAdmitted() >= 1
                            && value.metrics().requestsCompleted() >= 1
                            && value.metrics().workerRestarts() >= 1
                            && value.metrics().queueWait().count() >= 1
                            && value.metrics().requestExecution().count() >= 1,
                    "Snapshot metrics were incomplete: " + value.metrics());
            require(value.lastFailure() != null && value.lastFailure().message().length() <= 512,
                    "Bounded last failure was missing");
            System.out.printf("Phase 4 SNAPSHOT_METRICS passed: admitted=%d, completed=%d, "
                            + "workerRestarts=%d, healthSucceeded=%d, lastFailureCategory=%s%n",
                    value.metrics().requestsAdmitted(), value.metrics().requestsCompleted(),
                    value.metrics().workerRestarts(), value.metrics().healthChecksSucceeded(),
                    value.lastFailure().category());
        } finally {
            shutdownAndVerify(config, pids);
        }
    }

    private static void soak(ApplicationConfig base) throws Exception {
        ApplicationConfig config = configured(base, 4, 4, 2_000, 500, 0, 3, 5_000);
        ExecutorService callers = Executors.newFixedThreadPool(16);
        Set<Long> pids = Set.of();
        try {
            PythonCallUtil.initialize(PythonCallMode.MANAGED_RUNTIME, config);
            pids = waitForWorkerCount(4, 6_000);
            runBatch(config, callers, 100, "warmup");
            Map<Long, Integer> baselineFds = fdCounts(pids);
            Map<Long, Long> baselineRss = rssKilobytes(pids);
            for (int batch = 1; batch <= 5; batch++) {
                runBatch(config, callers, 1_000, "soak-" + batch);
                require(pids.equals(workerPids()), "Worker PID set changed during soak batch " + batch);
                ManagedPythonRuntimeSnapshot value = snapshot();
                require(value.queueDepth() == 0 && value.totalInFlight() == 0,
                        "Pending work leaked after soak batch " + batch + ": " + value);
            }
            Map<Long, Integer> finalFds = fdCounts(pids);
            Map<Long, Long> finalRss = rssKilobytes(pids);
            for (long pid : pids) {
                require(finalFds.get(pid) <= baselineFds.get(pid) + 2,
                        "File descriptors grew for PID " + pid + ": " + baselineFds + " -> " + finalFds);
                require(finalRss.get(pid) <= baselineRss.get(pid) + 32 * 1_024,
                        "RSS grew beyond 32 MiB for PID " + pid + ": " + baselineRss + " -> " + finalRss);
            }
            ManagedPythonRuntimeSnapshot value = snapshot();
            require(value.metrics().requestsCompleted() >= 5_100,
                    "Soak completion counter was lower than submitted work");
            System.out.printf("Phase 4 SOAK passed: pids=%s, completed=%d, baselineFds=%s, "
                            + "finalFds=%s, baselineRssKiB=%s, finalRssKiB=%s%n",
                    pids, value.metrics().requestsCompleted(), baselineFds, finalFds, baselineRss, finalRss);
        } finally {
            callers.shutdownNow();
            callers.awaitTermination(5, TimeUnit.SECONDS);
            shutdownAndVerify(config, pids);
        }
    }

    private static void parentTerminationHold(ApplicationConfig base) throws Exception {
        ApplicationConfig config = configured(base, 2, 1, 300, 100, 0, 3, 5_000);
        PythonCallUtil.initialize(PythonCallMode.MANAGED_RUNTIME, config);
        Set<Long> pids = waitForWorkerCount(2, 5_000);
        System.out.printf("Phase 4 PARENT_TERMINATION ready: javaPid=%d, workerPids=%s%n",
                ProcessHandle.current().pid(), pids);
        while (true) Thread.sleep(1_000);
    }

    private static void resourceRepetition(ApplicationConfig base) throws Exception {
        ApplicationConfig config = configured(base, 1, 1, 300, 100, 0, 5, 30_000);
        Set<Long> allPids = new HashSet<>();
        try {
            PythonCallUtil.initialize(PythonCallMode.MANAGED_RUNTIME, config);
            for (int iteration = 1; iteration <= 3; iteration++) {
                int expectedHealthChecks = iteration;
                waitUntil(() -> snapshot().metrics().healthChecksSucceeded() >= expectedHealthChecks, 3_000,
                        "Idle health did not complete in repetition " + iteration);
                long oldPid = onlyPid(waitForWorkerCount(1, 5_000));
                allPids.add(oldPid);
                kill(oldPid);
                int expectedRestarts = iteration;
                waitUntil(() -> snapshot().fullyReady()
                                && snapshot().metrics().workerRestarts() >= expectedRestarts
                                && workerPids().size() == 1 && !workerPids().contains(oldPid), 6_000,
                        "Worker was not replaced in repetition " + iteration);
                allPids.addAll(workerPids());
                request(config, "resource-repeat-" + iteration, 10);
            }
            System.out.printf("Phase 4 RESOURCE_REPETITION passed: observedPids=%s, workerRestarts=%d%n",
                    allPids, snapshot().metrics().workerRestarts());
        } finally {
            shutdownAndVerify(config, allPids);
        }
    }

    private static ApplicationConfig configured(
            ApplicationConfig base,
            int workers,
            int maxInFlight,
            long healthIntervalMs,
            long healthTimeoutMs,
            long healthGraceMs,
            int restartAttempts,
            long restartWindowMs) {
        ManagedPythonRuntimeConfig value = base.managedPythonRuntime();
        ManagedPythonRuntimeConfig managed = new ManagedPythonRuntimeConfig(
                value.pythonExecutable(), value.applicationDirectory(), value.udsDirectory(),
                workers, maxInFlight, Math.max(16, workers * maxInFlight * 2), 5_000,
                value.maxFrameBytes(), value.startupTimeoutMs(), value.requestTimeoutMs(),
                value.shutdownTimeoutMs(), true, 500, 500, restartAttempts, restartWindowMs,
                true, healthIntervalMs, healthTimeoutMs, healthGraceMs);
        return new ApplicationConfig(base.fastApi(), base.workload(), base.httpClient(), managed);
    }

    private static ApplicationConfig withPythonExecutable(ApplicationConfig config, String executable) {
        ManagedPythonRuntimeConfig value = config.managedPythonRuntime();
        ManagedPythonRuntimeConfig managed = new ManagedPythonRuntimeConfig(
                executable, value.applicationDirectory(), value.udsDirectory(), value.workerCount(),
                value.maxInFlightPerWorker(), value.queueCapacity(), value.queueTimeoutMs(),
                value.maxFrameBytes(), value.startupTimeoutMs(), value.requestTimeoutMs(),
                value.shutdownTimeoutMs(), value.restartEnabled(), value.restartInitialBackoffMs(),
                value.restartMaximumBackoffMs(), value.restartMaxAttempts(), value.restartWindowMs(),
                value.healthCheckEnabled(), value.healthCheckIntervalMs(), value.healthCheckTimeoutMs(),
                value.healthCheckStartupGraceMs());
        return new ApplicationConfig(config.fastApi(), config.workload(), config.httpClient(), managed);
    }

    private static ProcessResponse request(ApplicationConfig config, String requestId, int delayMs) throws Exception {
        Map<String, String> headers = new HashMap<>(config.fastApi().headers());
        headers.put("Content-Type", "application/json");
        headers.put("X-Request-Id", requestId);
        byte[] body = OBJECT_MAPPER.writeValueAsBytes(Map.of(
                "requestId", requestId, "message", "message-" + requestId, "delayMs", delayMs));
        PythonCallResponse response = PythonCallUtil.call(new PythonCallRequest(
                requestId, config.fastApi().method(), URI.create(config.fastApi().processUrl()), headers, body));
        require(response.statusCode() == 200, "Business request returned status " + response.statusCode());
        return OBJECT_MAPPER.readValue(response.body(), ProcessResponse.class);
    }

    private static void runBatch(
            ApplicationConfig config, ExecutorService callers, int count, String prefix) throws Exception {
        List<CompletableFuture<ProcessResponse>> futures = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            String requestId = prefix + '-' + index;
            futures.add(CompletableFuture.supplyAsync(() -> {
                try {
                    return request(config, requestId, 0);
                } catch (Exception failure) {
                    throw new CompletionException(failure);
                }
            }, callers));
        }
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).get(60, TimeUnit.SECONDS);
    }

    private static ManagedPythonRuntimeSnapshot snapshot() {
        return PythonCallUtil.managedRuntimeSnapshot()
                .orElseThrow(() -> new IllegalStateException("Managed runtime snapshot is unavailable"));
    }

    private static Throwable callFailure(ApplicationConfig config, String requestId, int delayMs) {
        try {
            request(config, requestId, delayMs);
            throw new IllegalStateException("Expected request to fail");
        } catch (Exception expected) {
            return expected;
        }
    }

    private static Set<Long> workerPids() {
        Set<Long> pids = new HashSet<>();
        ProcessHandle.current().descendants()
                .filter(ProcessHandle::isAlive)
                .filter(handle -> handle.info().commandLine().orElse("").contains("python_runtime.worker_runtime"))
                .forEach(handle -> pids.add(handle.pid()));
        return Set.copyOf(pids);
    }

    private static Set<Long> waitForWorkerCount(int expected, long timeoutMs) throws Exception {
        waitUntil(() -> workerPids().size() == expected, timeoutMs,
                "Expected " + expected + " workers, observed " + workerPids());
        return workerPids();
    }

    private static long onlyPid(Set<Long> pids) {
        require(pids.size() == 1, "Expected exactly one worker PID: " + pids);
        return pids.iterator().next();
    }

    private static void kill(long pid) {
        ProcessHandle handle = ProcessHandle.of(pid)
                .orElseThrow(() -> new IllegalStateException("Worker PID no longer exists: " + pid));
        require(handle.destroyForcibly(), "Unable to kill worker PID " + pid);
    }

    private static void signal(long pid, String signal) throws Exception {
        Process command = new ProcessBuilder("/bin/sh", "-c", "kill -" + signal + " " + pid).start();
        require(command.waitFor(2, TimeUnit.SECONDS) && command.exitValue() == 0,
                "Unable to send SIG" + signal + " to PID " + pid);
    }

    private static Map<Long, Integer> fdCounts(Set<Long> pids) throws Exception {
        Map<Long, Integer> counts = new HashMap<>();
        for (long pid : pids) {
            try (var paths = Files.list(Path.of("/proc", Long.toString(pid), "fd"))) {
                counts.put(pid, Math.toIntExact(paths.count()));
            }
        }
        return counts;
    }

    private static Map<Long, Long> rssKilobytes(Set<Long> pids) throws Exception {
        Map<Long, Long> values = new HashMap<>();
        for (long pid : pids) {
            String line = Files.readAllLines(Path.of("/proc", Long.toString(pid), "status")).stream()
                    .filter(value -> value.startsWith("VmRSS:"))
                    .findFirst().orElseThrow();
            values.put(pid, Long.parseLong(line.replaceAll("[^0-9]", "")));
        }
        return values;
    }

    private static void shutdownAndVerify(ApplicationConfig config, Set<Long> pids) throws Exception {
        PythonCallUtil.close();
        verifyPidsGone(pids, 3_000);
        verifyNoRuntimeDirectories(config);
    }

    private static void verifyPidsGone(Set<Long> pids, long timeoutMs) throws Exception {
        waitUntil(() -> pids.stream().noneMatch(pid ->
                        ProcessHandle.of(pid).map(ProcessHandle::isAlive).orElse(false)),
                timeoutMs, "Managed Python worker remained alive: " + pids);
    }

    private static void verifyNoRuntimeDirectories(ApplicationConfig config) throws Exception {
        Path parent = Path.of(config.managedPythonRuntime().udsDirectory()).toAbsolutePath().normalize();
        if (!Files.exists(parent)) return;
        try (var paths = Files.list(parent)) {
            require(paths.noneMatch(path -> path.getFileName().toString().startsWith("runtime-pool-")),
                    "Managed runtime directory was not removed from " + parent);
        }
    }

    private static boolean noManagedThreads() {
        return Thread.getAllStackTraces().keySet().stream()
                .filter(Thread::isAlive)
                .noneMatch(thread -> thread.getName().startsWith("managed-python-"));
    }

    private static void waitUntil(BooleanSupplier condition, long timeoutMs, String failure) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() >= deadline) throw new IllegalStateException(failure);
            Thread.sleep(25);
        }
    }

    private static String messageChain(Throwable failure) {
        StringBuilder result = new StringBuilder();
        for (Throwable current = failure; current != null; current = current.getCause()) {
            if (result.length() > 0) result.append(" | ");
            result.append(current.getMessage());
        }
        return result.toString();
    }

    private static void require(boolean condition, String message) {
        if (!condition) throw new IllegalStateException(message);
    }

    private record PythonFixture(Path directory, Path wrapper, Path driver, Path marker)
            implements AutoCloseable {
        private static PythonFixture stalePong() throws Exception {
            return create("stale-pong", """
                    sent_stale = False
                    async def patched(writer, metadata, body, limit):
                        global sent_stale
                        if metadata.get("type") == "pong" and not sent_stale:
                            sent_stale = True
                            stale = dict(metadata)
                            stale["workerGeneration"] = metadata["workerGeneration"] - 1
                            await production(writer, stale, body, limit)
                        await production(writer, metadata, body, limit)
                    """, false);
        }

        private static PythonFixture delayedPong() throws Exception {
            return create("delayed-pong", """
                    async def patched(writer, metadata, body, limit):
                        if metadata.get("type") == "pong":
                            await asyncio.sleep(1.0)
                        await production(writer, metadata, body, limit)
                    """, false);
        }

        private static PythonFixture failFirstStart() throws Exception {
            return create("fail-first", "", true);
        }

        private static PythonFixture create(String name, String patch, boolean failFirst) throws Exception {
            Path directory = Files.createTempDirectory("phase4-" + name + '-');
            Path wrapper = directory.resolve("python-wrapper");
            Path driver = directory.resolve("driver.py");
            Path marker = directory.resolve("first-invocation");
            Files.writeString(driver, """
                    import asyncio
                    import os
                    import sys
                    from pathlib import Path
                    sys.path.insert(0, os.getcwd())
                    from python_runtime import worker_runtime as runtime
                    def argument(name):
                        index = sys.argv.index(name)
                        return sys.argv[index + 1]
                    production = runtime.write_frame
                    %s
                    %s
                    asyncio.run(runtime.run_worker(
                        Path(argument("--socket-path")),
                        int(argument("--max-frame-bytes")),
                        int(argument("--max-in-flight-per-worker"))))
                    """.formatted(patch, patch.isBlank() ? "" : "runtime.write_frame = patched"));
            String first = failFirst
                    ? "if mkdir " + marker + " 2>/dev/null; then exit 17; fi\n" : "";
            Files.writeString(wrapper, "#!/bin/sh\nset -eu\n" + first
                    + (patch.isBlank() ? "exec python3 \"$@\"\n"
                    : "exec python3 " + driver + " \"$@\"\n"));
            require(wrapper.toFile().setExecutable(true, true), "Unable to make fixture executable");
            return new PythonFixture(directory, wrapper, driver, marker);
        }

        @Override
        public void close() throws Exception {
            Files.deleteIfExists(marker);
            Files.deleteIfExists(wrapper);
            Files.deleteIfExists(driver);
            Files.deleteIfExists(directory);
        }
    }
}
