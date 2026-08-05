package com.example.baseline.e2e;

import com.example.baseline.dto.ProcessResponse;
import com.example.baseline.utils.config.ApplicationConfig;
import com.example.baseline.utils.config.ApplicationConfig.ManagedPythonRuntimeConfig;
import com.example.baseline.utils.config.YamlConfigLoader;
import com.example.baseline.utils.python.PythonCallMode;
import com.example.baseline.utils.python.PythonCallRequest;
import com.example.baseline.utils.python.PythonCallResponse;
import com.example.baseline.utils.python.PythonCallUtil;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;

public final class Phase3E2ERunner {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private Phase3E2ERunner() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) throw new IllegalArgumentException("Usage: Phase3E2ERunner <SCENARIO>");
        ApplicationConfig base = YamlConfigLoader.load();
        switch (args[0]) {
            case "SAME_WORKER" -> sameWorker(base);
            case "MULTI_WORKER" -> multiWorker(base);
            case "OUT_OF_ORDER" -> outOfOrder(base);
            case "CAPACITY" -> capacity(base);
            case "MULTI_ACTIVE_FAILURE" -> multiActiveFailure(base);
            case "MULTI_ACTIVE_TIMEOUT" -> multiActiveTimeout(base);
            case "CORRELATED_ERROR" -> correlatedError(base);
            case "SHUTDOWN" -> shutdown(base);
            case "SHUTDOWN_TIMEOUT" -> shutdownTimeout(base);
            default -> throw new IllegalArgumentException("Unsupported Phase 3 E2E scenario: " + args[0]);
        }
    }

    private static void sameWorker(ApplicationConfig base) throws Exception {
        ApplicationConfig config = configured(base, 1, 4, 8, 2_000, 5_000, 2_000);
        ExecutorService callers = Executors.newFixedThreadPool(4);
        try {
            PythonCallUtil.initialize(PythonCallMode.MANAGED_RUNTIME, config);
            Set<Long> pids = waitForWorkerCount(1, 5_000);
            long start = System.nanoTime();
            List<Observation> results = successful(
                    requests(config, callers, List.of(500, 500, 500, 500), "same-worker", null), 5_000);
            double wallMs = elapsedMillis(start);
            int overlap = maximumOverlap(results);
            require(pids.equals(workerPids()), "Same-worker PID changed during normal execution");
            require(overlap == 4, "Expected four overlapping same-worker requests, observed " + overlap);
            require(wallMs < 1_500, "Same-worker execution remained serial: " + wallMs);
            verifyMappings(results);
            System.out.printf("Phase 3 SAME_WORKER passed: workerPids=%s, maxInFlight=4, "
                    + "maximumOverlap=%d, wallMs=%.3f, mappingsCorrect=true%n", pids, overlap, wallMs);
        } finally {
            shutdownAndVerify(config, callers);
        }
    }

    private static void multiWorker(ApplicationConfig base) throws Exception {
        ApplicationConfig config = configured(base, 2, 3, 8, 2_000, 5_000, 2_000);
        ExecutorService callers = Executors.newFixedThreadPool(6);
        try {
            PythonCallUtil.initialize(PythonCallMode.MANAGED_RUNTIME, config);
            Set<Long> pids = waitForWorkerCount(2, 5_000);
            long start = System.nanoTime();
            List<Observation> results = successful(
                    requests(config, callers, List.of(500, 500, 500, 500, 500, 500), "multi-worker", null),
                    5_000);
            double wallMs = elapsedMillis(start);
            int overlap = maximumOverlap(results);
            require(pids.equals(workerPids()), "Multi-worker PID set changed during normal execution");
            require(overlap == 6, "Expected six overlapping requests, observed " + overlap);
            require(wallMs < 1_500, "Two-worker multi-in-flight execution remained serial: " + wallMs);
            verifyMappings(results);
            System.out.printf("Phase 3 MULTI_WORKER passed: workerPids=%s, workers=2, maxPerWorker=3, "
                    + "maximumOverlap=%d, wallMs=%.3f, mappingsCorrect=true%n", pids, overlap, wallMs);
        } finally {
            shutdownAndVerify(config, callers);
        }
    }

    private static void outOfOrder(ApplicationConfig base) throws Exception {
        ApplicationConfig config = configured(base, 1, 4, 8, 2_000, 5_000, 2_000);
        ExecutorService callers = Executors.newFixedThreadPool(4);
        List<String> completionOrder = new CopyOnWriteArrayList<>();
        try {
            PythonCallUtil.initialize(PythonCallMode.MANAGED_RUNTIME, config);
            waitForWorkerCount(1, 5_000);
            List<String> submissionOrder = List.of("order-0", "order-1", "order-2", "order-3");
            List<CompletableFuture<Observation>> futures = requests(
                    config, callers, List.of(800, 50, 400, 100), "order", completionOrder);
            List<Observation> results = successful(futures, 5_000);
            verifyMappings(results);
            require(!completionOrder.equals(submissionOrder),
                    "Responses completed in submission order instead of demonstrating multiplexing");
            require(completionOrder.indexOf("order-1") < completionOrder.indexOf("order-0"),
                    "The 50 ms request did not complete before the 800 ms request");
            require(maximumOverlap(results) == 4, "Out-of-order requests did not all overlap");
            System.out.printf("Phase 3 OUT_OF_ORDER passed: submissionOrder=%s, completionOrder=%s, "
                    + "mappingsCorrect=true, maximumOverlap=4%n", submissionOrder, completionOrder);
        } finally {
            shutdownAndVerify(config, callers);
        }
    }

    private static void capacity(ApplicationConfig base) throws Exception {
        ApplicationConfig config = configured(base, 1, 2, 2, 250, 3_000, 2_000);
        ExecutorService callers = Executors.newFixedThreadPool(6);
        try {
            PythonCallUtil.initialize(PythonCallMode.MANAGED_RUNTIME, config);
            waitForWorkerCount(1, 5_000);
            long start = System.nanoTime();
            List<Outcome> outcomes = outcomes(
                    requests(config, callers, List.of(1_000, 1_000, 1_000, 1_000, 1_000, 1_000),
                            "capacity", null), 5_000);
            long failures = outcomes.stream().filter(outcome -> outcome.failure != null).count();
            List<Observation> successes = outcomes.stream()
                    .filter(outcome -> outcome.result != null).map(outcome -> outcome.result).toList();
            int overlap = maximumOverlap(successes);
            require(failures >= 3, "Expected bounded capacity/queue failures, observed " + failures);
            require(outcomes.stream().allMatch(Outcome::terminal), "A capacity future was not terminal");
            require(outcomes.stream().filter(outcome -> outcome.failure != null)
                            .allMatch(outcome -> messageChain(outcome.failure).contains("timed out")),
                    "A capacity failure did not report a timeout");
            require(overlap <= 2, "Worker exceeded max-in-flight=2; overlap=" + overlap);
            System.out.printf("Phase 3 CAPACITY passed: workerCount=1, maxInFlight=2, queueCapacity=2, "
                    + "calls=6, failures=%d, maximumOverlap=%d, allFuturesTerminal=true, elapsedMs=%d%n",
                    failures, overlap, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
        } finally {
            shutdownAndVerify(config, callers);
        }
    }

    private static void multiActiveFailure(ApplicationConfig base) throws Exception {
        ApplicationConfig config = configured(base, 2, 3, 8, 2_000, 5_000, 2_000);
        ExecutorService callers = Executors.newFixedThreadPool(6);
        try {
            PythonCallUtil.initialize(PythonCallMode.MANAGED_RUNTIME, config);
            Set<Long> before = waitForWorkerCount(2, 5_000);
            List<CompletableFuture<Observation>> futures = requests(
                    config, callers, List.of(1_500, 1_500, 1_500, 1_500, 1_500, 1_500), "worker-death", null);
            Thread.sleep(300);
            long killed = before.iterator().next();
            kill(killed);
            List<Outcome> outcomes = outcomes(futures, 6_000);
            long failures = outcomes.stream().filter(outcome -> outcome.failure != null).count();
            long successes = outcomes.stream().filter(outcome -> outcome.result != null).count();
            require(failures == 3 && successes == 3,
                    "Expected three failed assignments on the killed worker and three successes; failures="
                            + failures + ", successes=" + successes);
            require(outcomes.stream().allMatch(Outcome::terminal), "A worker-death future leaked");
            Set<Long> after = waitForChangedPool(before, 2, 8_000);
            Observation subsequent = request(config, "after-worker-death", 10, Map.of());
            require(subsequent.response.statusCode() == 200, "Replacement pool was not usable");
            System.out.printf("Phase 3 MULTI_ACTIVE_FAILURE passed: killedPid=%d, failedWithoutRetry=%d, "
                    + "otherWorkerSucceeded=%d, replacementPids=%s, allFuturesTerminal=true%n",
                    killed, failures, successes, after);
        } finally {
            shutdownAndVerify(config, callers);
        }
    }

    private static void multiActiveTimeout(ApplicationConfig base) throws Exception {
        ApplicationConfig config = configured(base, 1, 3, 6, 2_000, 300, 1_000);
        ExecutorService callers = Executors.newFixedThreadPool(3);
        try {
            PythonCallUtil.initialize(PythonCallMode.MANAGED_RUNTIME, config);
            Set<Long> before = waitForWorkerCount(1, 5_000);
            List<Outcome> outcomes = outcomes(
                    requests(config, callers, List.of(1_000, 700, 700), "timeout-sibling", null), 4_000);
            require(outcomes.stream().allMatch(outcome -> outcome.failure != null),
                    "A sibling assignment unexpectedly survived the poisoned worker");
            require(outcomes.stream().allMatch(Outcome::terminal), "A timeout sibling future leaked");
            Set<Long> after = waitForChangedPool(before, 1, 8_000);
            Observation subsequent = request(config, "after-timeout", 10, Map.of());
            require(subsequent.response.statusCode() == 200, "Replacement worker was not usable");
            System.out.printf("Phase 3 MULTI_ACTIVE_TIMEOUT passed: poisonedPid=%s, siblingFailures=3, "
                    + "replacementPid=%s, noRetry=true, allFuturesTerminal=true%n", before, after);
        } finally {
            shutdownAndVerify(config, callers);
        }
    }

    private static void correlatedError(ApplicationConfig base) throws Exception {
        ApplicationConfig config = configured(base, 1, 4, 8, 2_000, 5_000, 2_000);
        ExecutorService callers = Executors.newFixedThreadPool(4);
        try {
            PythonCallUtil.initialize(PythonCallMode.MANAGED_RUNTIME, config);
            Set<Long> before = waitForWorkerCount(1, 5_000);
            List<CompletableFuture<Observation>> siblings = requests(
                    config, callers, List.of(300, 300, 300), "error-sibling", null);
            CompletableFuture<Observation> invalid = CompletableFuture.supplyAsync(() -> {
                try {
                    return request(config, "correlated-error", 10, Map.of("X-E2E-Invalid", "€"));
                } catch (Exception failure) {
                    throw new CompletionException(failure);
                }
            }, callers);
            Throwable requestFailure;
            try {
                invalid.get(3, TimeUnit.SECONDS);
                throw new IllegalStateException("Expected correlated Python error");
            } catch (ExecutionException expected) {
                requestFailure = expected.getCause();
            }
            List<Observation> siblingResults = successful(siblings, 4_000);
            require(messageChain(requestFailure).contains("Managed Python Runtime error"),
                    "Request-specific error was not correlated: " + requestFailure);
            require(before.equals(workerPids()), "Request-specific error poisoned the worker");
            require(siblingResults.size() == 3, "A sibling request did not complete");
            Observation subsequent = request(config, "after-correlated-error", 10, Map.of());
            require(subsequent.response.statusCode() == 200, "Worker was not usable after correlated error");
            System.out.printf("Phase 3 CORRELATED_ERROR passed: workerPids=%s, failedRequests=1, "
                    + "siblingSuccesses=3, workerReused=true, releasedCapacity=true%n", before);
        } finally {
            shutdownAndVerify(config, callers);
        }
    }

    private static void shutdown(ApplicationConfig base) throws Exception {
        ApplicationConfig config = configured(base, 2, 3, 4, 2_000, 5_000, 2_000);
        ExecutorService callers = Executors.newFixedThreadPool(11);
        Set<Long> pids = Set.of();
        try {
            PythonCallUtil.initialize(PythonCallMode.MANAGED_RUNTIME, config);
            pids = waitForWorkerCount(2, 5_000);
            List<CompletableFuture<Observation>> futures = requests(
                    config, callers, List.of(500, 500, 500, 500, 500, 500, 500, 500, 500, 500),
                    "shutdown", null);
            Thread.sleep(200);
            long start = System.nanoTime();
            CompletableFuture<Void> closing = CompletableFuture.runAsync(PythonCallUtil::close, callers);
            Thread.sleep(50);
            Throwable rejected = callFailure(config, "shutdown-new", 1);
            List<Outcome> results = outcomes(futures, 5_000);
            closing.get(3, TimeUnit.SECONDS);
            long successes = results.stream().filter(outcome -> outcome.result != null).count();
            long failures = results.stream().filter(outcome -> outcome.failure != null).count();
            require(successes == 6 && failures == 4,
                    "Shutdown did not drain six active and fail four queued requests: successes="
                            + successes + ", failures=" + failures);
            require(results.stream().allMatch(Outcome::terminal), "A graceful-shutdown future leaked");
            require(messageChain(rejected).contains("SHUTTING_DOWN")
                            || messageChain(rejected).contains("CLOSED"),
                    "New work was not rejected during shutdown");
            verifyPidsGone(pids, 3_000);
            verifyNoRuntimeDirectories(config);
            System.out.printf("Phase 3 SHUTDOWN passed: drainedActive=%d, failedQueued=%d, "
                    + "newCallsRejected=true, allFuturesTerminal=true, shutdownMs=%d, removedPids=%s%n",
                    successes, failures, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start), pids);
        } finally {
            PythonCallUtil.close();
            callers.shutdownNow();
            callers.awaitTermination(5, TimeUnit.SECONDS);
            verifyPidsGone(pids, 3_000);
            verifyNoRuntimeDirectories(config);
        }
    }

    private static void shutdownTimeout(ApplicationConfig base) throws Exception {
        ApplicationConfig config = configured(base, 2, 3, 6, 1_000, 5_000, 500);
        ExecutorService callers = Executors.newFixedThreadPool(6);
        Set<Long> pids = Set.of();
        try {
            PythonCallUtil.initialize(PythonCallMode.MANAGED_RUNTIME, config);
            pids = waitForWorkerCount(2, 5_000);
            List<CompletableFuture<Observation>> futures = requests(
                    config, callers, List.of(2_000, 2_000, 2_000, 2_000, 2_000, 2_000),
                    "shutdown-timeout", null);
            Thread.sleep(200);
            long start = System.nanoTime();
            PythonCallUtil.close();
            long shutdownMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            List<Outcome> results = outcomes(futures, 2_000);
            require(results.stream().allMatch(outcome -> outcome.failure != null),
                    "An over-time assignment did not fail");
            require(results.stream().allMatch(Outcome::terminal), "A shutdown-timeout future leaked");
            require(shutdownMs < 1_500, "Shutdown multiplied the common deadline: " + shutdownMs);
            verifyPidsGone(pids, 3_000);
            verifyNoRuntimeDirectories(config);
            System.out.printf("Phase 3 SHUTDOWN_TIMEOUT passed: failedActive=6, commonDeadlineMs=500, "
                    + "shutdownMs=%d, allFuturesTerminal=true, removedPids=%s%n", shutdownMs, pids);
        } finally {
            PythonCallUtil.close();
            callers.shutdownNow();
            callers.awaitTermination(5, TimeUnit.SECONDS);
            verifyPidsGone(pids, 3_000);
            verifyNoRuntimeDirectories(config);
        }
    }

    private static List<CompletableFuture<Observation>> requests(
            ApplicationConfig config,
            ExecutorService callers,
            List<Integer> delays,
            String prefix,
            List<String> completionOrder) {
        List<CompletableFuture<Observation>> futures = new ArrayList<>();
        for (int index = 0; index < delays.size(); index++) {
            String requestId = prefix + '-' + index;
            int delay = delays.get(index);
            CompletableFuture<Observation> future = CompletableFuture.supplyAsync(() -> {
                try {
                    return request(config, requestId, delay, Map.of());
                } catch (Exception failure) {
                    throw new CompletionException(failure);
                }
            }, callers);
            if (completionOrder != null) future.whenComplete((ignored, failure) -> completionOrder.add(requestId));
            futures.add(future);
        }
        return futures;
    }

    private static Observation request(
            ApplicationConfig config, String requestId, int delayMs, Map<String, String> extraHeaders)
            throws Exception {
        Map<String, String> headers = new HashMap<>(config.fastApi().headers());
        headers.put("Content-Type", "application/json");
        headers.put("X-Request-Id", requestId);
        headers.putAll(extraHeaders);
        byte[] body = OBJECT_MAPPER.writeValueAsBytes(Map.of(
                "requestId", requestId,
                "message", "message-" + requestId,
                "delayMs", delayMs));
        PythonCallResponse response = PythonCallUtil.call(new PythonCallRequest(
                requestId,
                config.fastApi().method(),
                URI.create(config.fastApi().processUrl()),
                headers,
                body));
        ProcessResponse processResponse = response.statusCode() == 200
                ? OBJECT_MAPPER.readValue(response.body(), ProcessResponse.class)
                : null;
        return new Observation(requestId, response, processResponse);
    }

    private static List<Observation> successful(
            List<CompletableFuture<Observation>> futures, long timeoutMs) throws Exception {
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).get(timeoutMs, TimeUnit.MILLISECONDS);
        return futures.stream().map(CompletableFuture::join).toList();
    }

    private static List<Outcome> outcomes(
            List<CompletableFuture<Observation>> futures, long timeoutMs) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        List<Outcome> results = new ArrayList<>();
        for (CompletableFuture<Observation> future : futures) {
            try {
                results.add(new Outcome(future.get(Math.max(1, remainingMillis(deadline)), TimeUnit.MILLISECONDS), null));
            } catch (ExecutionException failure) {
                results.add(new Outcome(null, failure.getCause()));
            }
        }
        return results;
    }

    private static void verifyMappings(List<Observation> results) {
        for (Observation result : results) {
            require(result.response.statusCode() == 200, "Request did not return status 200: " + result.requestId);
            require(result.processResponse != null && result.requestId.equals(result.processResponse.requestId()),
                    "Response mapping was incorrect for " + result.requestId);
            require(("message-" + result.requestId).toUpperCase(Locale.ROOT)
                            .equals(result.processResponse.processedMessage()),
                    "Business response was incorrect for " + result.requestId);
        }
    }

    private static int maximumOverlap(List<Observation> results) {
        List<Instant> starts = results.stream()
                .filter(result -> result.processResponse != null)
                .map(result -> Instant.parse(result.processResponse.pythonStartTime())).toList();
        List<Instant> ends = results.stream()
                .filter(result -> result.processResponse != null)
                .map(result -> Instant.parse(result.processResponse.pythonEndTime())).toList();
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

    private static ApplicationConfig configured(
            ApplicationConfig base,
            int workers,
            int maxInFlight,
            int queueCapacity,
            long queueTimeoutMs,
            long requestTimeoutMs,
            long shutdownTimeoutMs) {
        ManagedPythonRuntimeConfig value = base.managedPythonRuntime();
        ManagedPythonRuntimeConfig managed = new ManagedPythonRuntimeConfig(
                value.pythonExecutable(), value.applicationDirectory(), value.udsDirectory(),
                workers, maxInFlight, queueCapacity, queueTimeoutMs, value.maxFrameBytes(),
                value.startupTimeoutMs(), requestTimeoutMs, shutdownTimeoutMs,
                true, 50, 200, 3, 5_000);
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
                "Expected " + count + " workers, observed " + workerPids());
        return workerPids();
    }

    private static Set<Long> waitForChangedPool(Set<Long> before, int count, long timeoutMs) throws Exception {
        waitUntil(() -> workerPids().size() == count && !workerPids().equals(before), timeoutMs,
                "Worker pool did not replace a generation; before=" + before + ", current=" + workerPids());
        return workerPids();
    }

    private static void kill(long pid) {
        ProcessHandle handle = ProcessHandle.of(pid)
                .orElseThrow(() -> new IllegalStateException("Worker PID no longer exists: " + pid));
        require(handle.destroyForcibly(), "Unable to kill worker PID " + pid);
    }

    private static Throwable callFailure(ApplicationConfig config, String requestId, int delayMs) {
        try {
            request(config, requestId, delayMs, Map.of());
            throw new IllegalStateException("Expected request to fail");
        } catch (Exception expected) {
            return expected;
        }
    }

    private static void shutdownAndVerify(ApplicationConfig config, ExecutorService executor) throws Exception {
        Set<Long> pids = workerPids();
        PythonCallUtil.close();
        executor.shutdownNow();
        executor.awaitTermination(5, TimeUnit.SECONDS);
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

    private static double elapsedMillis(long startNs) {
        return (System.nanoTime() - startNs) / 1_000_000.0;
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

    private record Observation(
            String requestId,
            PythonCallResponse response,
            ProcessResponse processResponse) {
    }

    private record Outcome(Observation result, Throwable failure) {
        private boolean terminal() {
            return result != null || failure != null;
        }
    }
}
