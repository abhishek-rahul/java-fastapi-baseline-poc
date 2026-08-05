package com.example.baseline.utils.http;

import com.example.baseline.utils.config.ApplicationConfig.HttpClientConfig;
import okhttp3.ConnectionPool;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public final class HttpUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpUtil.class);
    private static final MediaType JSON = MediaType.get("application/json; charset=utf-8");
    private static final Set<String> BODY_FORBIDDEN_METHODS = Set.of("GET", "HEAD");
    private static final Set<String> BODY_REQUIRED_METHODS = Set.of("POST", "PUT", "PATCH");

    private static volatile OkHttpClient aiHttpClient;

    private HttpUtil() {
    }

    public static synchronized void initialize(HttpClientConfig config) {
        if (aiHttpClient != null) throw new IllegalStateException("HttpUtil is already initialized");
        aiHttpClient = new OkHttpClient.Builder()
                .connectTimeout(Duration.ofMillis(config.connectTimeoutMs()))
                .readTimeout(Duration.ofMillis(config.readTimeoutMs()))
                .writeTimeout(Duration.ofMillis(config.writeTimeoutMs()))
                .callTimeout(Duration.ofMillis(config.callTimeoutMs()))
                .connectionPool(new ConnectionPool(
                        config.maxIdleConnections(),
                        config.keepAliveDurationMs(),
                        TimeUnit.MILLISECONDS
                ))
                .build();
    }

    /** The caller owns the returned response and must close it. */
    public static Response callExternal(
            String url,
            String method,
            JSONObject payload,
            Map<String, String> headers
    ) throws Exception {
        if (url == null || url.isBlank()) throw new IllegalArgumentException("url must not be blank");
        if (method == null || method.isBlank()) throw new IllegalArgumentException("method must not be blank");

        String normalizedMethod = method.toUpperCase(Locale.ROOT);
        if (BODY_FORBIDDEN_METHODS.contains(normalizedMethod) && payload != null) {
            throw new IllegalArgumentException(normalizedMethod + " requests cannot contain a payload");
        }

        RequestBody requestBody = payload == null ? null : RequestBody.create(payload.toString(), JSON);
        if (requestBody == null && BODY_REQUIRED_METHODS.contains(normalizedMethod)) {
            requestBody = RequestBody.create("", JSON);
        }

        Request.Builder request = new Request.Builder().url(url).method(normalizedMethod, requestBody);
        if (headers != null) {
            headers.forEach(request::header);
        }

        //LOGGER.info("Making API call to url: {} with method: {} with req body: {}", url, normalizedMethod, payload);
        return client().newCall(request.build()).execute();
    }

    public static synchronized void close() {
        OkHttpClient client = aiHttpClient;
        aiHttpClient = null;
        if (client == null) return;
        client.dispatcher().executorService().shutdown();
        client.connectionPool().evictAll();
        if (client.cache() != null) {
            try {
                client.cache().close();
            } catch (IOException exception) {
                LOGGER.warn("Unable to close HTTP cache", exception);
            }
        }
    }

    private static OkHttpClient client() {
        OkHttpClient client = aiHttpClient;
        if (client == null) throw new IllegalStateException("HttpUtil must be initialized before use");
        return client;
    }
}
