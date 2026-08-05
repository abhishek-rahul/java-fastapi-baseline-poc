package com.example.baseline.dto;

public record ProcessRequest(
        String requestId,
        String message,
        int delayMs
) {
}
