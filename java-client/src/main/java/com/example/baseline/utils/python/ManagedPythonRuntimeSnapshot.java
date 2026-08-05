package com.example.baseline.utils.python;

import java.util.List;

public record ManagedPythonRuntimeSnapshot(
        String poolState,
        boolean accepting,
        boolean ready,
        boolean fullyReady,
        int configuredWorkers,
        int responsiveWorkers,
        int restartingWorkers,
        int exhaustedWorkers,
        int queueDepth,
        int queueCapacity,
        int totalInFlight,
        int maximumInFlight,
        int availableCapacity,
        Metrics metrics,
        FailureSummary lastFailure,
        List<Worker> workers
) {
    public ManagedPythonRuntimeSnapshot {
        workers = List.copyOf(workers);
    }

    public record Metrics(
            long requestsAdmitted,
            long requestsCompleted,
            long requestsFailed,
            long queueFullObservations,
            long queueTimeouts,
            long preTransmissionTimeouts,
            long postTransmissionTimeouts,
            long workerCrashes,
            long workerRestarts,
            long restartExhaustions,
            long healthChecksSent,
            long healthChecksSucceeded,
            long healthChecksFailed,
            long forcedTerminations,
            long cleanupFailures,
            Timer queueWait,
            Timer requestExecution,
            Timer workerStartup,
            Timer workerReplacement,
            Timer poolShutdown
    ) {
    }

    public record Timer(long count, long totalNanos, long maximumNanos) {
    }

    public record FailureSummary(
            String category,
            String workerId,
            int generation,
            long pid,
            String message,
            long occurredAtEpochMillis
    ) {
    }

    public record Worker(
            String workerId,
            int generation,
            long pid,
            String state,
            boolean processAlive,
            boolean responsive,
            boolean dispatchEligible,
            int inFlight,
            int maximumInFlight,
            boolean healthCheckOutstanding,
            long lastResponsiveEpochMillis,
            long lastHealthLatencyNanos,
            boolean restarting,
            boolean exhausted
    ) {
    }
}
