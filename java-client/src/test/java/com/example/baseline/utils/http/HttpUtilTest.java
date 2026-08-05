package com.example.baseline.utils.http;

import com.example.baseline.utils.config.ApplicationConfig.HttpClientConfig;
import okhttp3.Response;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class HttpUtilTest {
    private MockWebServer server;

    @BeforeEach
    void setUp() throws IOException {
        server = new MockWebServer();
        server.start();
        HttpUtil.initialize(new HttpClientConfig(1_000, 1_000, 1_000, 1_000, 2, 10_000));
    }

    @AfterEach
    void tearDown() throws IOException {
        HttpUtil.close();
        server.shutdown();
    }

    @Test
    void sendsMethodPayloadAndHeaders() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(200).setBody("{\"ok\":true}"));
        JSONObject payload = new JSONObject().put("message", "hello");

        try (Response response = HttpUtil.callExternal(
                server.url("/api/test").toString(), "post", payload, Map.of("X-Test", "value"))) {
            assertEquals(200, response.code());
        }

        RecordedRequest request = server.takeRequest();
        assertEquals("POST", request.getMethod());
        assertEquals("/api/test", request.getPath());
        assertEquals("value", request.getHeader("X-Test"));
        assertNotNull(request.getHeader("Content-Type"));
        assertEquals("hello", new JSONObject(request.getBody().readUtf8()).getString("message"));
    }

    @Test
    void reusesTheInitializedClientAcrossCalls() throws Exception {
        server.enqueue(new MockResponse());
        server.enqueue(new MockResponse());

        try (Response ignored = HttpUtil.callExternal(server.url("/one").toString(), "GET", null, null)) {
            // Response ownership remains with the caller.
        }
        try (Response ignored = HttpUtil.callExternal(server.url("/two").toString(), "GET", null, null)) {
            // Response ownership remains with the caller.
        }

        assertEquals(2, server.getRequestCount());
    }

    @Test
    void rejectsPayloadForGetAndHead() {
        JSONObject payload = new JSONObject().put("key", "value");
        assertThrows(IllegalArgumentException.class,
                () -> HttpUtil.callExternal(server.url("/").toString(), "GET", payload, null));
        assertThrows(IllegalArgumentException.class,
                () -> HttpUtil.callExternal(server.url("/").toString(), "HEAD", payload, null));
    }
}
