package com.example.baseline.utils.config;

import com.fasterxml.jackson.annotation.JsonProperty;

import com.example.baseline.utils.python.PythonCallMode;

import java.nio.file.Path;
import java.util.Map;

public record ApplicationConfig(
        @JsonProperty("fast-api") FastApiConfig fastApi,
        WorkloadConfig workload,
        @JsonProperty("http-client") HttpClientConfig httpClient,
        @JsonProperty("managed-python-runtime") ManagedPythonRuntimeConfig managedPythonRuntime
) {
    public ApplicationConfig {
        if (fastApi == null || workload == null) {
            throw new IllegalArgumentException("fast-api and workload configuration are required");
        }
    }

    public void validateFor(PythonCallMode mode) {
        if (mode == PythonCallMode.HTTP && httpClient == null) {
            throw new IllegalArgumentException("http-client configuration is required for HTTP mode");
        }
        if (mode == PythonCallMode.HTTP) httpClient.validate();
        if (mode == PythonCallMode.MANAGED_RUNTIME && managedPythonRuntime == null) {
            throw new IllegalArgumentException(
                    "managed-python-runtime configuration is required for MANAGED_RUNTIME mode");
        }
        if (mode == PythonCallMode.MANAGED_RUNTIME) managedPythonRuntime.validate();
    }

    public record FastApiConfig(
            @JsonProperty("process-url") String processUrl,
            String method,
            Map<String, String> headers
    ) {
        public FastApiConfig {
            requireText(processUrl, "fast-api.process-url");
            requireText(method, "fast-api.method");
            headers = headers == null ? Map.of() : Map.copyOf(headers);
        }
    }

    public record WorkloadConfig(
            @JsonProperty("thread-pool-size") int threadPoolSize,
            @JsonProperty("request-count") int requestCount,
            @JsonProperty("delay-ms") int delayMs
    ) {
        public WorkloadConfig {
            if (threadPoolSize <= 0) throw new IllegalArgumentException("workload.thread-pool-size must be greater than zero");
            if (requestCount <= 0) throw new IllegalArgumentException("workload.request-count must be greater than zero");
            if (delayMs < 0 || delayMs > 10_000) throw new IllegalArgumentException("workload.delay-ms must be between 0 and 10000");
        }
    }

    public record HttpClientConfig(
            @JsonProperty("connect-timeout-ms") long connectTimeoutMs,
            @JsonProperty("read-timeout-ms") long readTimeoutMs,
            @JsonProperty("write-timeout-ms") long writeTimeoutMs,
            @JsonProperty("call-timeout-ms") long callTimeoutMs,
            @JsonProperty("max-idle-connections") int maxIdleConnections,
            @JsonProperty("keep-alive-duration-ms") long keepAliveDurationMs
    ) {
        public void validate() {
            requireNonNegative(connectTimeoutMs, "http-client.connect-timeout-ms");
            requireNonNegative(readTimeoutMs, "http-client.read-timeout-ms");
            requireNonNegative(writeTimeoutMs, "http-client.write-timeout-ms");
            requireNonNegative(callTimeoutMs, "http-client.call-timeout-ms");
            if (maxIdleConnections < 0) throw new IllegalArgumentException("http-client.max-idle-connections must not be negative");
            if (keepAliveDurationMs <= 0) throw new IllegalArgumentException("http-client.keep-alive-duration-ms must be greater than zero");
        }
    }

    public record ManagedPythonRuntimeConfig(
            @JsonProperty("python-executable") String pythonExecutable,
            @JsonProperty("application-directory") String applicationDirectory,
            @JsonProperty("uds-directory") String udsDirectory,
            @JsonProperty("worker-count") int workerCount,
            @JsonProperty("queue-capacity") int queueCapacity,
            @JsonProperty("queue-timeout-ms") long queueTimeoutMs,
            @JsonProperty("max-frame-bytes") int maxFrameBytes,
            @JsonProperty("startup-timeout-ms") long startupTimeoutMs,
            @JsonProperty("request-timeout-ms") long requestTimeoutMs,
            @JsonProperty("shutdown-timeout-ms") long shutdownTimeoutMs,
            @JsonProperty("restart-enabled") boolean restartEnabled,
            @JsonProperty("restart-initial-backoff-ms") long restartInitialBackoffMs,
            @JsonProperty("restart-maximum-backoff-ms") long restartMaximumBackoffMs,
            @JsonProperty("restart-max-attempts") int restartMaxAttempts,
            @JsonProperty("restart-window-ms") long restartWindowMs
    ) {
        public void validate() {
            requireText(pythonExecutable, "managed-python-runtime.python-executable");
            requireText(applicationDirectory, "managed-python-runtime.application-directory");
            requireText(udsDirectory, "managed-python-runtime.uds-directory");
            if (workerCount < 1 || workerCount > 64) {
                throw new IllegalArgumentException(
                        "managed-python-runtime.worker-count must be between 1 and 64");
            }
            if (queueCapacity <= 0) {
                throw new IllegalArgumentException(
                        "managed-python-runtime.queue-capacity must be greater than zero");
            }
            requirePositive(queueTimeoutMs, "managed-python-runtime.queue-timeout-ms");
            if (maxFrameBytes < 1024) {
                throw new IllegalArgumentException(
                        "managed-python-runtime.max-frame-bytes must be at least 1024");
            }
            requirePositive(startupTimeoutMs, "managed-python-runtime.startup-timeout-ms");
            requirePositive(requestTimeoutMs, "managed-python-runtime.request-timeout-ms");
            requirePositive(shutdownTimeoutMs, "managed-python-runtime.shutdown-timeout-ms");
            if (restartInitialBackoffMs < 0) {
                throw new IllegalArgumentException(
                        "managed-python-runtime.restart-initial-backoff-ms must not be negative");
            }
            if (restartMaximumBackoffMs < restartInitialBackoffMs) {
                throw new IllegalArgumentException(
                        "managed-python-runtime.restart-maximum-backoff-ms must be at least restart-initial-backoff-ms");
            }
            if (restartEnabled && restartMaxAttempts < 1) {
                throw new IllegalArgumentException(
                        "managed-python-runtime.restart-max-attempts must be at least 1 when restart is enabled");
            }
            if (!restartEnabled && restartMaxAttempts < 0) {
                throw new IllegalArgumentException(
                        "managed-python-runtime.restart-max-attempts must not be negative");
            }
            requirePositive(restartWindowMs, "managed-python-runtime.restart-window-ms");
        }

        public ManagedPythonRuntimeConfig normalized() {
            validate();
            return new ManagedPythonRuntimeConfig(
                    pythonExecutable,
                    Path.of(applicationDirectory).toAbsolutePath().normalize().toString(),
                    Path.of(udsDirectory).toAbsolutePath().normalize().toString(),
                    workerCount,
                    queueCapacity,
                    queueTimeoutMs,
                    maxFrameBytes,
                    startupTimeoutMs,
                    requestTimeoutMs,
                    shutdownTimeoutMs,
                    restartEnabled,
                    restartInitialBackoffMs,
                    restartMaximumBackoffMs,
                    restartMaxAttempts,
                    restartWindowMs
            );
        }
    }

    private static void requireText(String value, String property) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException(property + " must not be blank");
    }

    private static void requireNonNegative(long value, String property) {
        if (value < 0) throw new IllegalArgumentException(property + " must not be negative");
    }

    private static void requirePositive(long value, String property) {
        if (value <= 0) throw new IllegalArgumentException(property + " must be greater than zero");
    }
}
