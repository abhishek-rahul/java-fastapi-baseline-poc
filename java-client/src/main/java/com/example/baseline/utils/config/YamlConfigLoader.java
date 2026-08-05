package com.example.baseline.utils.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.IOException;
import java.io.InputStream;

public final class YamlConfigLoader {
    private static final String DEFAULT_RESOURCE = "application.yml";
    private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());

    private YamlConfigLoader() {
    }

    public static ApplicationConfig load() {
        return load(DEFAULT_RESOURCE);
    }

    static ApplicationConfig load(String resourceName) {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        try (InputStream input = classLoader.getResourceAsStream(resourceName)) {
            if (input == null) throw new IllegalStateException("Configuration resource not found: " + resourceName);
            return YAML_MAPPER.readValue(input, ApplicationConfig.class);
        } catch (IOException | IllegalArgumentException exception) {
            throw new IllegalStateException("Unable to load configuration resource: " + resourceName, exception);
        }
    }
}
