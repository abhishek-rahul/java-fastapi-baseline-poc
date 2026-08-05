package com.example.baseline.client;

import com.example.baseline.dto.CallResult;
import com.example.baseline.dto.ProcessRequest;
import com.example.baseline.dto.ProcessResponse;
import com.example.baseline.utils.config.ApplicationConfig.FastApiConfig;
import com.example.baseline.utils.http.HttpUtil;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import okhttp3.Response;
import org.json.JSONObject;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public final class RestProcessingClient {
    private final FastApiConfig config;
    private final ObjectMapper objectMapper;

    public RestProcessingClient(FastApiConfig config) {
        this.config = config;
        this.objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    public CallResult process(ProcessRequest payload) throws Exception {
        JSONObject requestPayload = new JSONObject()
                .put("requestId", payload.requestId())
                .put("message", payload.message())
                .put("delayMs", payload.delayMs());
        Map<String, String> headers = new HashMap<>(config.headers());
        headers.put("X-Request-Id", payload.requestId());

        Instant startTime = Instant.now();
        long startNs = System.nanoTime();

        try (Response response = HttpUtil.callExternal(
                config.processUrl(), config.method(), requestPayload, headers)) {
            long endNs = System.nanoTime();
            Instant endTime = Instant.now();
            String responseBody = response.body() == null ? "" : response.body().string();

            if (!response.isSuccessful()) {
                throw new IOException(
                        "HTTP request failed. status=" + response.code() + ", body=" + responseBody
                );
            }

            ProcessResponse processResponse = objectMapper.readValue(responseBody, ProcessResponse.class);
            return new CallResult(
                    payload.requestId(),
                    response.code(),
                    startTime,
                    endTime,
                    (endNs - startNs) / 1_000_000.0,
                    Thread.currentThread().getName(),
                    processResponse
            );
        }
    }

}
