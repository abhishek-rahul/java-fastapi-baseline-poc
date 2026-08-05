package com.example.baseline.utils.python;

import java.util.Locale;

public enum PythonCallMode {
    HTTP,
    MANAGED_RUNTIME;

    public static PythonCallMode fromArguments(String[] args) {
        if (args == null || args.length == 0) return HTTP;
        if (args.length > 1) {
            throw new IllegalArgumentException("Expected at most one mode argument: HTTP or MANAGED_RUNTIME");
        }
        try {
            return valueOf(args[0].trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException exception) {
            throw new IllegalArgumentException(
                    "Unsupported mode '" + args[0] + "'. Expected HTTP or MANAGED_RUNTIME", exception);
        }
    }
}
