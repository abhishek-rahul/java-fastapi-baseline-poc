package com.example.baseline.utils.config;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public record ApplicationConfig(
        @JsonProperty("fast-api") FastApiConfig fastApi,
        WorkloadConfig workload,
        @JsonProperty("http-client") HttpClientConfig httpClient
) {
    public ApplicationConfig {
        if (fastApi == null || workload == null || httpClient == null) {
            throw new IllegalArgumentException("fast-api, workload and http-client configuration are required");
        }
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
        public HttpClientConfig {
            requireNonNegative(connectTimeoutMs, "http-client.connect-timeout-ms");
            requireNonNegative(readTimeoutMs, "http-client.read-timeout-ms");
            requireNonNegative(writeTimeoutMs, "http-client.write-timeout-ms");
            requireNonNegative(callTimeoutMs, "http-client.call-timeout-ms");
            if (maxIdleConnections < 0) throw new IllegalArgumentException("http-client.max-idle-connections must not be negative");
            if (keepAliveDurationMs <= 0) throw new IllegalArgumentException("http-client.keep-alive-duration-ms must be greater than zero");
        }
    }

    private static void requireText(String value, String property) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException(property + " must not be blank");
    }

    private static void requireNonNegative(long value, String property) {
        if (value < 0) throw new IllegalArgumentException(property + " must not be negative");
    }
}
