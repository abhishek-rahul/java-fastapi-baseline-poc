package com.example.baseline.e2e;

import com.example.baseline.client.RestProcessingClient;
import com.example.baseline.dto.CallResult;
import com.example.baseline.dto.ProcessRequest;
import com.example.baseline.utils.config.ApplicationConfig;
import com.example.baseline.utils.config.YamlConfigLoader;
import com.example.baseline.utils.python.PythonCallMode;
import com.example.baseline.utils.python.PythonCallRequest;
import com.example.baseline.utils.python.PythonCallResponse;
import com.example.baseline.utils.python.PythonCallUtil;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public final class Phase1E2ERunner {
    private Phase1E2ERunner() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1 || args.length > 2) {
            throw new IllegalArgumentException(
                    "Usage: Phase1E2ERunner HTTP|MANAGED_RUNTIME [STANDARD|SHUTDOWN_DRAIN|SHUTDOWN_TIMEOUT]");
        }
        PythonCallMode mode = PythonCallMode.fromArguments(new String[]{args[0]});
        ApplicationConfig config = YamlConfigLoader.load();
        String scenario = args.length == 1 ? "STANDARD" : args[1];
        if (!"STANDARD".equals(scenario)) {
            if (mode != PythonCallMode.MANAGED_RUNTIME) {
                throw new IllegalArgumentException("Shutdown scenarios require MANAGED_RUNTIME mode");
            }
            if ("SHUTDOWN_DRAIN".equals(scenario)) {
                verifyShutdownDrain(config);
                return;
            }
            if ("SHUTDOWN_TIMEOUT".equals(scenario)) {
                verifyShutdownTimeout(config);
                return;
            }
            throw new IllegalArgumentException("Unsupported E2E scenario: " + scenario);
        }
        ExecutorService callers = Executors.newFixedThreadPool(4);
        try {
            PythonCallUtil.initialize(mode, config);
            PythonCallUtil.initialize(mode, config);
            verifyConflictingInitialization(mode, config);
            verifyRawSuccess(config);
            verifyValidationAndNonSuccess(config);
            verifyConcurrentCalls(config, callers);
            System.out.printf("Phase 1 E2E passed: mode=%s, sharedRuntime=true, concurrentCallers=4%n", mode);
        } finally {
            callers.shutdownNow();
            callers.awaitTermination(5, TimeUnit.SECONDS);
            PythonCallUtil.close();
            PythonCallUtil.close();
        }
        verifyPostCloseRejection(config);
    }

    private static void verifyShutdownDrain(ApplicationConfig config) throws Exception {
        ExecutorService callers = Executors.newFixedThreadPool(3);
        try {
            PythonCallUtil.initialize(PythonCallMode.MANAGED_RUNTIME, config);
            RestProcessingClient client = new RestProcessingClient(config.fastApi());
            CompletableFuture<CallResult> active = processAsync(
                    client, new ProcessRequest("shutdown-active", "active", 1_000), callers);
            Thread.sleep(250);
            CompletableFuture<CallResult> queued = processAsync(
                    client, new ProcessRequest("shutdown-queued", "queued", 100), callers);
            Thread.sleep(100);

            long shutdownStart = System.nanoTime();
            CompletableFuture<Void> shutdown = CompletableFuture.runAsync(PythonCallUtil::close, callers);
            Thread.sleep(50);
            verifyCallRejectedDuringShutdown(config);

            CallResult activeResult = active.get(3, TimeUnit.SECONDS);
            require(activeResult.httpStatus() == 200, "Active request did not finish successfully during drain");
            Throwable queuedFailure = failureFrom(queued, 3, TimeUnit.SECONDS);
            require(messageChain(queuedFailure).contains("shutting down"),
                    "Queued request did not receive a shutdown failure: " + queuedFailure);
            shutdown.get(6, TimeUnit.SECONDS);
            double shutdownMs = (System.nanoTime() - shutdownStart) / 1_000_000.0;
            require(active.isDone() && queued.isDone() && shutdown.isDone(),
                    "A shutdown-drain future did not reach a terminal state");
            System.out.printf(
                    "Phase 1 shutdown drain passed: activeCompleted=true, queuedFailed=true, " +
                            "newCallsRejected=true, shutdownMs=%.3f%n",
                    shutdownMs);
        } finally {
            PythonCallUtil.close();
            callers.shutdownNow();
            callers.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    private static void verifyShutdownTimeout(ApplicationConfig config) throws Exception {
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try {
            PythonCallUtil.initialize(PythonCallMode.MANAGED_RUNTIME, config);
            RestProcessingClient client = new RestProcessingClient(config.fastApi());
            CompletableFuture<CallResult> active = processAsync(
                    client, new ProcessRequest("shutdown-timeout", "timeout", 10_000), caller);
            Thread.sleep(250);

            long shutdownStart = System.nanoTime();
            PythonCallUtil.close();
            double shutdownMs = (System.nanoTime() - shutdownStart) / 1_000_000.0;
            Throwable activeFailure = failureFrom(active, 2, TimeUnit.SECONDS);
            require(messageChain(activeFailure).contains("exceeded the shutdown timeout"),
                    "Active request did not receive the deterministic shutdown-timeout failure: " + activeFailure);
            require(shutdownMs < config.managedPythonRuntime().shutdownTimeoutMs() + 1_500,
                    "Shutdown exceeded its bounded allowance: " + shutdownMs + " ms");
            require(active.isDone(), "Timed-out active request future was not completed");
            System.out.printf(
                    "Phase 1 shutdown timeout passed: activeFailed=true, futureDone=true, shutdownMs=%.3f%n",
                    shutdownMs);
        } finally {
            PythonCallUtil.close();
            caller.shutdownNow();
            caller.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    private static CompletableFuture<CallResult> processAsync(
            RestProcessingClient client,
            ProcessRequest request,
            ExecutorService executor
    ) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return client.process(request);
            } catch (Exception exception) {
                throw new CompletionException(exception);
            }
        }, executor);
    }

    private static void verifyCallRejectedDuringShutdown(ApplicationConfig config) {
        try {
            PythonCallUtil.call(request(config, "{}"));
            throw new IllegalStateException("New call was accepted after shutdown began");
        } catch (IllegalStateException expected) {
            require(expected.getMessage().contains("SHUTTING_DOWN") || expected.getMessage().contains("CLOSED"),
                    "New-call rejection did not report shutdown state: " + expected.getMessage());
        } catch (Exception exception) {
            throw new IllegalStateException("Unexpected new-call rejection", exception);
        }
    }

    private static Throwable failureFrom(
            CompletableFuture<?> future,
            long timeout,
            TimeUnit unit
    ) throws InterruptedException, TimeoutException {
        try {
            future.get(timeout, unit);
            throw new IllegalStateException("Expected future to fail");
        } catch (ExecutionException exception) {
            return exception.getCause();
        }
    }

    private static String messageChain(Throwable throwable) {
        StringBuilder messages = new StringBuilder();
        Throwable current = throwable;
        while (current != null) {
            if (current.getMessage() != null) messages.append(current.getMessage()).append(' ');
            current = current.getCause();
        }
        return messages.toString();
    }

    private static void verifyConflictingInitialization(PythonCallMode mode, ApplicationConfig config) {
        PythonCallMode conflicting = mode == PythonCallMode.HTTP
                ? PythonCallMode.MANAGED_RUNTIME
                : PythonCallMode.HTTP;
        try {
            PythonCallUtil.initialize(conflicting, config);
            throw new IllegalStateException("Conflicting initialization was not rejected");
        } catch (IllegalStateException expected) {
            if (!expected.getMessage().contains("different configuration")) throw expected;
        }
    }

    private static void verifyRawSuccess(ApplicationConfig config) throws Exception {
        String body = """
                {"requestId":"e2e-valid","message":"phase-one","delayMs":1}
                """;
        PythonCallResponse response = PythonCallUtil.call(request(config, body));
        require(response.statusCode() == 200, "Expected status 200, received " + response.statusCode());
        require(response.bodyAsUtf8().contains("\"processedMessage\":\"PHASE-ONE\""),
                "Successful response did not contain the expected business value");
        require(response.headers().keySet().stream().anyMatch(name -> name.equalsIgnoreCase("content-type")),
                "Successful response did not contain Content-Type");
    }

    private static void verifyValidationAndNonSuccess(ApplicationConfig config) throws Exception {
        String invalidBody = """
                {"requestId":"","message":"","delayMs":-1}
                """;
        PythonCallResponse response = PythonCallUtil.call(request(config, invalidBody));
        require(response.statusCode() == 422, "Expected validation status 422, received " + response.statusCode());
        require(response.bodyAsUtf8().contains("detail"), "Validation response did not contain FastAPI details");

        RestProcessingClient client = new RestProcessingClient(config.fastApi());
        try {
            client.process(new ProcessRequest("", "", -1));
            throw new IllegalStateException("RestProcessingClient did not reject a non-success response");
        } catch (IOException expected) {
            require(expected.getMessage().contains("status=422"), "Caller did not preserve status 422");
        }
    }

    private static void verifyConcurrentCalls(ApplicationConfig config, ExecutorService callers) {
        RestProcessingClient client = new RestProcessingClient(config.fastApi());
        List<CompletableFuture<CallResult>> futures = new ArrayList<>();
        for (int index = 0; index < 8; index++) {
            int requestNumber = index;
            futures.add(CompletableFuture.supplyAsync(() -> {
                try {
                    return client.process(new ProcessRequest(
                            "e2e-concurrent-" + requestNumber,
                            "message-" + requestNumber,
                            5));
                } catch (Exception exception) {
                    throw new IllegalStateException(exception);
                }
            }, callers));
        }
        List<CallResult> results = futures.stream().map(CompletableFuture::join).toList();
        require(results.size() == 8, "Expected eight concurrent-call results");
        require(results.stream().allMatch(result -> result.httpStatus() == 200),
                "A concurrent call did not return status 200");
    }

    private static PythonCallRequest request(ApplicationConfig config, String body) {
        Map<String, String> headers = new HashMap<>(config.fastApi().headers());
        headers.put("Content-Type", "application/json");
        headers.put("X-Request-Id", "phase-1-e2e");
        return new PythonCallRequest(
                UUID.randomUUID().toString(),
                config.fastApi().method(),
                URI.create(config.fastApi().processUrl()),
                headers,
                body.getBytes(StandardCharsets.UTF_8));
    }

    private static void verifyPostCloseRejection(ApplicationConfig config) {
        try {
            PythonCallUtil.call(request(config, "{}"));
            throw new IllegalStateException("Call after shutdown was not rejected");
        } catch (IllegalStateException expected) {
            require(expected.getMessage().contains("current state=CLOSED"),
                    "Post-close failure did not report the closed state");
        } catch (Exception exception) {
            throw new IllegalStateException("Unexpected post-close failure", exception);
        }
    }

    private static void require(boolean condition, String message) {
        if (!condition) throw new IllegalStateException(message);
    }
}
