package com.example.baseline.utils.python;

import com.example.baseline.utils.config.ApplicationConfig.HttpClientConfig;
import com.example.baseline.utils.http.HttpUtil;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.json.JSONObject;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.nio.charset.StandardCharsets;

final class HttpPythonCallExecutor implements PythonCallExecutor {
    HttpPythonCallExecutor(HttpClientConfig config) {
        HttpUtil.initialize(config);
    }

    @Override
    public PythonCallResponse call(PythonCallRequest request) throws Exception {
        byte[] requestBody = request.body();
        JSONObject payload = requestBody.length == 0
                ? null
                : new JSONObject(new String(requestBody, StandardCharsets.UTF_8));
        try (Response response = HttpUtil.callExternal(
                request.target().toString(), request.method(), payload, request.headers())) {
            ResponseBody responseBody = response.body();
            byte[] body = responseBody == null ? new byte[0] : responseBody.bytes();
            Map<String, List<String>> headers = new LinkedHashMap<>();
            response.headers().toMultimap().forEach((name, values) -> headers.put(name, List.copyOf(values)));
            return new PythonCallResponse(response.code(), headers, body);
        }
    }

    @Override
    public void close() {
        HttpUtil.close();
    }
}
