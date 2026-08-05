package com.example.baseline.utils.python;

import com.example.baseline.utils.config.ApplicationConfig;

public final class PythonCallUtil {
    private static final Object LIFECYCLE_LOCK = new Object();

    private static LifecycleState state = LifecycleState.NEW;
    private static EffectiveConfiguration effectiveConfiguration;
    private static PythonCallExecutor executor;
    private static Throwable initializationFailure;

    private PythonCallUtil() {
    }

    public static void initialize(PythonCallMode mode, ApplicationConfig config) {
        if (mode == null) throw new IllegalArgumentException("mode must not be null");
        if (config == null) throw new IllegalArgumentException("config must not be null");
        config.validateFor(mode);
        EffectiveConfiguration requested = EffectiveConfiguration.from(mode, config);

        synchronized (LIFECYCLE_LOCK) {
            awaitInitialization();
            if (state == LifecycleState.READY) {
                if (requested.equals(effectiveConfiguration)) return;
                throw new IllegalStateException("PythonCallUtil is already initialized with different configuration");
            }
            if (state == LifecycleState.FAILED) {
                throw new IllegalStateException("PythonCallUtil initialization previously failed", initializationFailure);
            }
            if (state == LifecycleState.SHUTTING_DOWN || state == LifecycleState.CLOSED) {
                throw new IllegalStateException("PythonCallUtil cannot be initialized while " + state);
            }
            state = LifecycleState.INITIALIZING;
        }

        PythonCallExecutor created = null;
        try {
            created = mode == PythonCallMode.HTTP
                    ? new HttpPythonCallExecutor(config.httpClient())
                    : ManagedPythonRuntime.start(config.managedPythonRuntime().normalized());
            synchronized (LIFECYCLE_LOCK) {
                executor = created;
                effectiveConfiguration = requested;
                initializationFailure = null;
                state = LifecycleState.READY;
                LIFECYCLE_LOCK.notifyAll();
            }
        } catch (Throwable failure) {
            if (created != null) created.close();
            synchronized (LIFECYCLE_LOCK) {
                initializationFailure = failure;
                state = LifecycleState.FAILED;
                LIFECYCLE_LOCK.notifyAll();
            }
            if (failure instanceof RuntimeException runtimeException) throw runtimeException;
            throw new IllegalStateException("Unable to initialize PythonCallUtil", failure);
        }
    }

    public static PythonCallResponse call(PythonCallRequest request) throws Exception {
        PythonCallExecutor selected;
        synchronized (LIFECYCLE_LOCK) {
            if (state != LifecycleState.READY) {
                throw new IllegalStateException("PythonCallUtil is not ready; current state=" + state,
                        state == LifecycleState.FAILED ? initializationFailure : null);
            }
            selected = executor;
        }
        return selected.call(request);
    }

    public static void close() {
        PythonCallExecutor selected;
        synchronized (LIFECYCLE_LOCK) {
            awaitInitialization();
            if (state == LifecycleState.NEW || state == LifecycleState.CLOSED) {
                state = LifecycleState.CLOSED;
                return;
            }
            if (state == LifecycleState.SHUTTING_DOWN) return;
            state = LifecycleState.SHUTTING_DOWN;
            selected = executor;
            executor = null;
        }
        try {
            if (selected != null) selected.close();
        } finally {
            synchronized (LIFECYCLE_LOCK) {
                state = LifecycleState.CLOSED;
                LIFECYCLE_LOCK.notifyAll();
            }
        }
    }

    private static void awaitInitialization() {
        boolean interrupted = false;
        while (state == LifecycleState.INITIALIZING) {
            try {
                LIFECYCLE_LOCK.wait();
            } catch (InterruptedException exception) {
                interrupted = true;
            }
        }
        if (interrupted) Thread.currentThread().interrupt();
    }

    private enum LifecycleState {
        NEW,
        INITIALIZING,
        READY,
        FAILED,
        SHUTTING_DOWN,
        CLOSED
    }

    private record EffectiveConfiguration(PythonCallMode mode, Object selectedConfiguration) {
        static EffectiveConfiguration from(PythonCallMode mode, ApplicationConfig config) {
            Object selected = mode == PythonCallMode.HTTP
                    ? config.httpClient()
                    : config.managedPythonRuntime().normalized();
            return new EffectiveConfiguration(mode, selected);
        }
    }
}
