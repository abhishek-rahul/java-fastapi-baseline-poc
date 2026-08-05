package com.example.baseline;

import com.example.baseline.client.RestProcessingClient;
import com.example.baseline.dto.CallResult;
import com.example.baseline.dto.ProcessRequest;
import com.example.baseline.utils.config.ApplicationConfig;
import com.example.baseline.utils.config.YamlConfigLoader;
import com.example.baseline.utils.python.PythonCallMode;
import com.example.baseline.utils.python.PythonCallUtil;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public final class BaselineApplication {

    private BaselineApplication() {
    }

    public static void main(String[] args) {
        PythonCallMode mode = PythonCallMode.fromArguments(args);
        ApplicationConfig config = YamlConfigLoader.load();
        config.validateFor(mode);
        ApplicationConfig.WorkloadConfig workload = config.workload();
        PythonCallUtil.initialize(mode, config);
        ExecutorService requestExecutor = Executors.newFixedThreadPool(
                workload.threadPoolSize(),
                runnable -> {
                    Thread thread = new Thread(runnable);
                    thread.setName("java-request-worker-" + thread.getId());
                    return thread;
                }
        );

        System.out.printf(
                "Starting baseline: mode=%s, processUrl=%s, method=%s, threadPoolSize=%d, requestCount=%d, delayMs=%d%n",
                mode,
                config.fastApi().processUrl(),
                config.fastApi().method(),
                workload.threadPoolSize(),
                workload.requestCount(),
                workload.delayMs()
        );

        Instant batchStartTime = Instant.now();
        long batchStartNs = System.nanoTime();

        try {
            RestProcessingClient client = new RestProcessingClient(config.fastApi());
            List<CompletableFuture<CallResult>> futures = new ArrayList<>();

            for (int index = 1; index <= workload.requestCount(); index++) {
                int requestNumber = index;
                CompletableFuture<CallResult> future = CompletableFuture.supplyAsync(() -> {
                    ProcessRequest request = new ProcessRequest(
                            "request-" + requestNumber,
                            "hello-from-java-" + requestNumber,
                            workload.delayMs()
                    );
                    try {
                        return client.process(request);
                    } catch (Exception exception) {
                        throw new CompletionException(exception);
                    }
                }, requestExecutor);
                futures.add(future);
            }

            List<CallResult> results = futures.stream()
                    .map(CompletableFuture::join)
                    .sorted(Comparator.comparing(CallResult::requestId))
                    .toList();

            long batchEndNs = System.nanoTime();
            Instant batchEndTime = Instant.now();
            double batchDurationMs = (batchEndNs - batchStartNs) / 1_000_000.0;

            //results.forEach(BaselineApplication::printResult);

            double averageObservedMs = results.stream()
                    .mapToDouble(CallResult::javaObservedTimeMs)
                    .average()
                    .orElse(0.0);

            double averagePythonMs = results.stream()
                    .mapToDouble(result -> result.response().pythonExecutionTimeMs())
                    .average()
                    .orElse(0.0);

            System.out.println();
            System.out.printf("Batch start time         : %s%n", batchStartTime);
            System.out.printf("Batch end time           : %s%n", batchEndTime);
            System.out.printf("Batch wall-clock time ms : %.3f%n", batchDurationMs);
            System.out.printf("Average Java observed ms : %.3f%n", averageObservedMs);
            System.out.printf("Average Python work ms   : %.3f%n", averagePythonMs);
            System.out.printf("Successful requests      : %d%n", results.size());
        } finally {
            requestExecutor.shutdown();
            try {
                if (!requestExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                    requestExecutor.shutdownNow();
                }
            } catch (InterruptedException interruptedException) {
                requestExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
            PythonCallUtil.close();
        }
    }

    private static void printResult(CallResult result) {
        System.out.println();
        System.out.printf("Request ID               : %s%n", result.requestId());
        System.out.printf("Java worker thread       : %s%n", result.javaWorkerThread());
        System.out.printf("Java start time          : %s%n", result.javaStartTime());
        System.out.printf("Java end time            : %s%n", result.javaEndTime());
        System.out.printf("Java observed time ms    : %.3f%n", result.javaObservedTimeMs());
        System.out.printf("HTTP status              : %d%n", result.httpStatus());
        System.out.printf("Python start time        : %s%n", result.response().pythonStartTime());
        System.out.printf("Python end time          : %s%n", result.response().pythonEndTime());
        System.out.printf("Python execution time ms : %.3f%n", result.response().pythonExecutionTimeMs());
        System.out.printf("Python event-loop thread : %s%n", result.response().eventLoopThread());
        System.out.printf("Processed message        : %s%n", result.response().processedMessage());
    }
}
