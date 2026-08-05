package com.example.baseline.client;

import com.example.baseline.dto.CallResult;
import com.example.baseline.dto.ProcessRequest;
import com.example.baseline.dto.ProcessResponse;
import com.example.baseline.utils.config.ApplicationConfig.FastApiConfig;
import com.example.baseline.utils.python.PythonCallRequest;
import com.example.baseline.utils.python.PythonCallResponse;
import com.example.baseline.utils.python.PythonCallUtil;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.json.JSONObject;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

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

        PythonCallResponse response = PythonCallUtil.call(new PythonCallRequest(
                UUID.randomUUID().toString(),
                config.method(),
                URI.create(config.processUrl()),
                headers,
                requestPayload.toString().getBytes(StandardCharsets.UTF_8)
        ));
        long endNs = System.nanoTime();
        Instant endTime = Instant.now();
        String responseBody = response.bodyAsUtf8();

        if (!response.isSuccessful()) {
            throw new IOException(
                    "HTTP request failed. status=" + response.statusCode() + ", body=" + responseBody
            );
        }

        ProcessResponse processResponse = objectMapper.readValue(responseBody, ProcessResponse.class);
        return new CallResult(
                payload.requestId(),
                response.statusCode(),
                startTime,
                endTime,
                (endNs - startNs) / 1_000_000.0,
                Thread.currentThread().getName(),
                processResponse
        );
    }

}
