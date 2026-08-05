package com.example.baseline.utils.python;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public record PythonCallResponse(
        int statusCode,
        Map<String, List<String>> headers,
        byte[] body
) {
    public PythonCallResponse {
        if (statusCode < 100 || statusCode > 999) throw new IllegalArgumentException("Invalid status code: " + statusCode);
        headers = headers == null ? Map.of() : Map.copyOf(headers);
        body = body == null ? new byte[0] : body.clone();
    }

    @Override
    public byte[] body() {
        return body.clone();
    }

    public String bodyAsUtf8() {
        return new String(body, StandardCharsets.UTF_8);
    }

    public boolean isSuccessful() {
        return statusCode >= 200 && statusCode < 300;
    }
}
