package com.example.baseline.dto;

public record ProcessResponse(
        String requestId,
        String originalMessage,
        String processedMessage,
        int delayMs,
        String pythonStartTime,
        String pythonEndTime,
        double pythonExecutionTimeMs,
        String eventLoopThread
) {
}
