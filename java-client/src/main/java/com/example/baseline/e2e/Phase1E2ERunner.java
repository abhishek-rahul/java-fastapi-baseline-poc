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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public final class Phase1E2ERunner {
    private Phase1E2ERunner() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) throw new IllegalArgumentException("Usage: Phase1E2ERunner HTTP|MANAGED_RUNTIME");
        PythonCallMode mode = PythonCallMode.fromArguments(args);
        ApplicationConfig config = YamlConfigLoader.load();
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
