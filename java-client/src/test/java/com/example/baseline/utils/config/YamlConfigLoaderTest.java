package com.example.baseline.utils.config;

import org.junit.jupiter.api.Test;

import java.util.Map;

import com.example.baseline.utils.python.PythonCallMode;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class YamlConfigLoaderTest {

    @Test
    void loadsClasspathApplicationConfiguration() {
        ApplicationConfig config = YamlConfigLoader.load();

        assertEquals("http://127.0.0.1:8000/api/v1/process", config.fastApi().processUrl());
        assertEquals("POST", config.fastApi().method());
        assertEquals(4, config.workload().threadPoolSize());
        assertEquals(5_000, config.httpClient().connectTimeoutMs());
    }

    @Test
    void rejectsMissingConfigurationResource() {
        assertThrows(IllegalStateException.class, () -> YamlConfigLoader.load("missing.yml"));
    }

    @Test
    void validatesConfigurationValues() {
        assertThrows(IllegalArgumentException.class,
                () -> new ApplicationConfig.FastApiConfig(" ", "POST", Map.of()));
        assertThrows(IllegalArgumentException.class,
                () -> new ApplicationConfig.WorkloadConfig(0, 1, 10));
        assertThrows(IllegalArgumentException.class,
                () -> new ApplicationConfig.WorkloadConfig(1, 0, 10));
        assertThrows(IllegalArgumentException.class,
                () -> new ApplicationConfig.WorkloadConfig(1, 1, -1));
        ApplicationConfig.FastApiConfig fastApi = new ApplicationConfig.FastApiConfig(
                "http://127.0.0.1:8000/api/v1/process", "POST", Map.of());
        ApplicationConfig.WorkloadConfig workload = new ApplicationConfig.WorkloadConfig(1, 1, 1);
        assertThrows(IllegalArgumentException.class, () -> new ApplicationConfig(
                fastApi, workload,
                new ApplicationConfig.HttpClientConfig(-1, 1, 1, 1, 1, 1), null
        ).validateFor(PythonCallMode.HTTP));
        assertThrows(IllegalArgumentException.class, () -> new ApplicationConfig(
                fastApi, workload,
                new ApplicationConfig.HttpClientConfig(1, 1, 1, 1, -1, 1), null
        ).validateFor(PythonCallMode.HTTP));
        assertThrows(IllegalArgumentException.class, () -> new ApplicationConfig(
                fastApi, workload,
                new ApplicationConfig.HttpClientConfig(1, 1, 1, 1, 1, 0), null
        ).validateFor(PythonCallMode.HTTP));
    }
}
