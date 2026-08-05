package com.example.baseline.utils.python;

import com.example.baseline.utils.config.ApplicationConfig;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

public final class PythonCallUtil {
    private static final Object LIFECYCLE_LOCK = new Object();

    private static LifecycleState state = LifecycleState.NEW;
    private static EffectiveConfiguration effectiveConfiguration;
    private static PythonCallExecutor executor;
    private static Throwable initializationFailure;
    private static Thread initializationThread;
    private static long initializationShutdownTimeoutMs = 5_000;
    private static boolean shutdownRequested;

    static {
        Runtime.getRuntime().addShutdownHook(
                new Thread(PythonCallUtil::close, "managed-python-jvm-shutdown"));
    }

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
            initializationThread = Thread.currentThread();
            shutdownRequested = false;
            if (mode == PythonCallMode.MANAGED_RUNTIME) {
                initializationShutdownTimeoutMs = config.managedPythonRuntime().shutdownTimeoutMs();
            }
        }

        PythonCallExecutor created = null;
        try {
            created = mode == PythonCallMode.HTTP
                    ? new HttpPythonCallExecutor(config.httpClient())
                    : ManagedPythonRuntime.start(config.managedPythonRuntime().normalized());
            boolean publish;
            synchronized (LIFECYCLE_LOCK) {
                initializationThread = null;
                publish = state == LifecycleState.INITIALIZING && !shutdownRequested;
                if (publish) {
                    executor = created;
                    effectiveConfiguration = requested;
                    initializationFailure = null;
                    state = LifecycleState.READY;
                }
                LIFECYCLE_LOCK.notifyAll();
            }
            if (!publish) {
                created.close();
                synchronized (LIFECYCLE_LOCK) {
                    state = LifecycleState.CLOSED;
                    LIFECYCLE_LOCK.notifyAll();
                }
                throw new IllegalStateException("PythonCallUtil initialization was cancelled by shutdown");
            }
        } catch (Throwable failure) {
            if (created != null) created.close();
            synchronized (LIFECYCLE_LOCK) {
                initializationThread = null;
                if (state == LifecycleState.SHUTTING_DOWN || shutdownRequested) {
                    state = LifecycleState.CLOSED;
                } else {
                    initializationFailure = failure;
                    state = LifecycleState.FAILED;
                }
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

    public static Optional<ManagedPythonRuntimeSnapshot> managedRuntimeSnapshot() {
        PythonCallExecutor selected;
        synchronized (LIFECYCLE_LOCK) {
            if (state != LifecycleState.READY) return Optional.empty();
            selected = executor;
        }
        return selected instanceof ManagedPythonRuntime runtime
                ? Optional.of(runtime.snapshot()) : Optional.empty();
    }

    public static void close() {
        PythonCallExecutor selected = null;
        Thread initializer = null;
        long deadlineNs = Long.MAX_VALUE;
        synchronized (LIFECYCLE_LOCK) {
            if (state == LifecycleState.NEW || state == LifecycleState.CLOSED) {
                state = LifecycleState.CLOSED;
                return;
            }
            if (state == LifecycleState.SHUTTING_DOWN) return;
            if (state == LifecycleState.INITIALIZING) {
                shutdownRequested = true;
                state = LifecycleState.SHUTTING_DOWN;
                initializer = initializationThread;
                deadlineNs = System.nanoTime()
                        + TimeUnit.MILLISECONDS.toNanos(initializationShutdownTimeoutMs);
            }
            if (state != LifecycleState.SHUTTING_DOWN) state = LifecycleState.SHUTTING_DOWN;
            selected = executor;
            executor = null;
        }
        if (initializer != null && initializer != Thread.currentThread()) {
            initializer.interrupt();
            synchronized (LIFECYCLE_LOCK) {
                boolean interrupted = false;
                while (initializationThread != null && System.nanoTime() < deadlineNs) {
                    try {
                        long millis = Math.max(1,
                                TimeUnit.NANOSECONDS.toMillis(deadlineNs - System.nanoTime()));
                        LIFECYCLE_LOCK.wait(millis);
                    } catch (InterruptedException exception) {
                        interrupted = true;
                        break;
                    }
                }
                if (interrupted) Thread.currentThread().interrupt();
            }
        }
        try {
            if (selected != null) selected.close();
        } finally {
            synchronized (LIFECYCLE_LOCK) {
                state = LifecycleState.CLOSED;
                initializationThread = null;
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
