package com.example.baseline.client;

import com.example.baseline.dto.CallResult;
import com.example.baseline.dto.ProcessRequest;
import com.example.baseline.utils.config.ApplicationConfig.FastApiConfig;
import com.example.baseline.utils.config.ApplicationConfig.HttpClientConfig;
import com.example.baseline.utils.http.HttpUtil;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RestProcessingClientTest {
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
    void mapsSuccessfulFastApiResponse() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(200).setBody("""
                {"requestId":"request-1","originalMessage":"hello","processedMessage":"HELLO",
                "delayMs":10,"pythonStartTime":"start","pythonEndTime":"end",
                "pythonExecutionTimeMs":10.5,"eventLoopThread":"MainThread"}
                """));
        RestProcessingClient client = client();

        CallResult result = client.process(new ProcessRequest("request-1", "hello", 10));

        assertEquals(200, result.httpStatus());
        assertEquals("HELLO", result.response().processedMessage());
        assertEquals("request-1", server.takeRequest().getHeader("X-Request-Id"));
    }

    @Test
    void reportsNonSuccessfulResponse() {
        server.enqueue(new MockResponse().setResponseCode(500).setBody("failure details"));

        IOException exception = assertThrows(IOException.class,
                () -> client().process(new ProcessRequest("request-1", "hello", 10)));

        assertTrue(exception.getMessage().contains("status=500"));
        assertTrue(exception.getMessage().contains("failure details"));
    }

    private RestProcessingClient client() {
        return new RestProcessingClient(new FastApiConfig(
                server.url("/api/v1/process").toString(), "POST", Map.of("Accept", "application/json")));
    }
}
