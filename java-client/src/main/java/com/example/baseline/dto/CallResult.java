package com.example.baseline.dto;

import java.time.Instant;

public record CallResult(
        String requestId,
        int httpStatus,
        Instant javaStartTime,
        Instant javaEndTime,
        double javaObservedTimeMs,
        String javaWorkerThread,
        ProcessResponse response
) {
}
