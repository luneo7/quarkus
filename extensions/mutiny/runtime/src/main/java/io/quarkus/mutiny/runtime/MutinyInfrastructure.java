package io.quarkus.mutiny.runtime;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.jboss.logging.Logger;
import org.jboss.threads.ContextHandler;

import io.quarkus.runtime.ShutdownContext;
import io.quarkus.runtime.annotations.Recorder;
import io.quarkus.runtime.configuration.QuarkusContextHandler;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.mutiny.infrastructure.MutinyScheduler;

@Recorder
public class MutinyInfrastructure {

    public static final String VERTX_EVENT_LOOP_THREAD_PREFIX = "vert.x-eventloop-thread-";

    public void configureMutinyInfrastructure(ExecutorService exec, QuarkusContextHandler<Object> contextHandler,
            ShutdownContext shutdownContext) {
        //mutiny leaks a ScheduledExecutorService if you don't do this
        Infrastructure.getDefaultWorkerPool().shutdown();
        Infrastructure.setDefaultExecutor(new Executor() {
            @Override
            public void execute(Runnable command) {
                try {
                    exec.execute(command);
                } catch (RejectedExecutionException e) {
                    if (!exec.isShutdown() && !exec.isTerminated()) {
                        throw e;
                    }
                    // Ignore the failure - the application has been shutdown.
                }
            }
        }, new MutinyScheduler(exec) {
            @Override
            public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
                return super.schedule(
                        new ContextualRunnable(contextHandler, contextHandler.captureContext(), command),
                        delay,
                        unit);
            }

            @Override
            public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
                return super.schedule(
                        new ContextualCallable<>(contextHandler, contextHandler.captureContext(), callable),
                        delay,
                        unit);
            }

            @Override
            public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
                return super.scheduleAtFixedRate(
                        new ContextualRunnable(contextHandler, contextHandler.captureContext(), command),
                        initialDelay,
                        period,
                        unit);
            }

            @Override
            public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
                return super.scheduleWithFixedDelay(
                        new ContextualRunnable(contextHandler, contextHandler.captureContext(), command),
                        initialDelay,
                        delay,
                        unit);
            }

            @Override
            public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
                return super.invokeAny(decorateTasks(tasks));
            }

            @Override
            public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
                    throws InterruptedException, ExecutionException, TimeoutException {
                return super.invokeAny(decorateTasks(tasks), timeout, unit);
            }

            @Override
            public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
                return super.invokeAll(decorateTasks(tasks));
            }

            @Override
            public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
                    throws InterruptedException {
                return super.invokeAll(decorateTasks(tasks), timeout, unit);
            }

            private <T> List<Callable<T>> decorateTasks(Collection<? extends Callable<T>> tasks) {
                return tasks
                        .stream()
                        .map(c -> new ContextualCallable<>(contextHandler, contextHandler.captureContext(), c))
                        .collect(Collectors.toList());
            }
        });
        shutdownContext.addLastShutdownTask(new Runnable() {
            @Override
            public void run() {
                Infrastructure.getDefaultWorkerPool().shutdown();
            }
        });
    }

    public void configureDroppedExceptionHandler() {
        Logger logger = Logger.getLogger(MutinyInfrastructure.class);
        Infrastructure.setDroppedExceptionHandler(new Consumer<Throwable>() {
            @Override
            public void accept(Throwable throwable) {
                logger.error("Mutiny had to drop the following exception", throwable);
            }
        });
    }

    public void configureThreadBlockingChecker() {
        Infrastructure.setCanCallerThreadBeBlockedSupplier(new BooleanSupplier() {
            @Override
            public boolean getAsBoolean() {
                /*
                 * So far all threads and Vert.x worker threads can block, but Vert.x event-loop threads must not block.
                 * It is safe to detect Vert.x event-loop threads by naming convention.
                 *
                 * It also avoids adding a dependency of this extension on the Vert.x APIs to check if we are
                 * calling from a Vert.x event-loop context / thread.
                 */
                String threadName = Thread.currentThread().getName();
                return !threadName.startsWith(VERTX_EVENT_LOOP_THREAD_PREFIX);
            }
        });
    }

    public void configureOperatorLogger() {
        Logger logger = Logger.getLogger(MutinyInfrastructure.class);
        Infrastructure.setOperatorLogger(new Infrastructure.OperatorLogger() {
            @Override
            public void log(String identifier, String event, Object value, Throwable failure) {
                String log = identifier + " | ";
                if (failure != null) {
                    log = log + event + "(" + failure.getClass() + "(" + failure.getMessage() + "))";
                } else if (value != null) {
                    log = log + event + "(" + value + ")";
                } else {
                    log = log + event + "()";
                }
                logger.info(log);
            }
        });
    }

    public static final class ContextualRunnable implements Runnable {
        private final Runnable runnable;
        private final Object context;
        private final ContextHandler<Object> contextHandler;

        public ContextualRunnable(ContextHandler<Object> contextHandler, Object context, Runnable runnable) {
            this.contextHandler = contextHandler;
            this.context = context;
            this.runnable = runnable;
        }

        @Override
        public void run() {
            contextHandler.runWith(runnable, context);
        }
    }

    public static final class ContextualCallable<V> implements Callable<V> {
        private final Callable<V> callable;
        private final Object context;
        private final QuarkusContextHandler<Object> contextHandler;

        public ContextualCallable(QuarkusContextHandler<Object> contextHandler, Object context, Callable<V> callable) {
            this.contextHandler = contextHandler;
            this.context = context;
            this.callable = callable;
        }

        @Override
        public V call() throws Exception {
            return contextHandler.callWith(callable, context);
        }
    }
}
