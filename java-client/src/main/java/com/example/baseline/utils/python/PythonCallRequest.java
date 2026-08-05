package com.example.baseline.utils.python;

import java.net.URI;
import java.util.Map;

public record PythonCallRequest(
        String requestId,
        String method,
        URI target,
        Map<String, String> headers,
        byte[] body
) {
    public PythonCallRequest {
        if (requestId == null || requestId.isBlank()) throw new IllegalArgumentException("requestId must not be blank");
        if (method == null || method.isBlank()) throw new IllegalArgumentException("method must not be blank");
        if (target == null) throw new IllegalArgumentException("target must not be null");
        headers = headers == null ? Map.of() : Map.copyOf(headers);
        body = body == null ? new byte[0] : body.clone();
    }

    @Override
    public byte[] body() {
        return body.clone();
    }
}
