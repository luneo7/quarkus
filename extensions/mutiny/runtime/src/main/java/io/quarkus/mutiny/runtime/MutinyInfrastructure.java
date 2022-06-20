package io.quarkus.mutiny.runtime;

import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RunnableScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import org.jboss.logging.Logger;
import org.jboss.threads.ContextHandler;

import io.quarkus.runtime.ShutdownContext;
import io.quarkus.runtime.annotations.Recorder;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.mutiny.infrastructure.MutinyScheduler;

@Recorder
public class MutinyInfrastructure {

    public static final String VERTX_EVENT_LOOP_THREAD_PREFIX = "vert.x-eventloop-thread-";

    public void configureMutinyInfrastructure(ExecutorService exec, ContextHandler<Object> contextHandler,
            ShutdownContext shutdownContext) {
        //mutiny leaks a ScheduledExecutorService if you don't do this
        Infrastructure.getDefaultWorkerPool().shutdown();

        MutinyScheduler mutinyScheduler = contextHandler != null ? new MutinyScheduler(exec) {
            @Override
            protected <V> RunnableScheduledFuture<V> decorateTask(Runnable runnable, RunnableScheduledFuture<V> task) {
                return super.decorateTask(runnable, new ContextualRunnableScheduledFuture<>(contextHandler,
                                                                                            contextHandler.captureContext(),
                                                                                            task));
            }

            @Override
            protected <V> RunnableScheduledFuture<V> decorateTask(Callable<V> callable, RunnableScheduledFuture<V> task) {
                return super.decorateTask(callable, new ContextualRunnableScheduledFuture<>(contextHandler,
                                                                                            contextHandler.captureContext(),
                                                                                            task));
            }
        } : new MutinyScheduler(exec);

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
        }, mutinyScheduler);

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

    public static class ContextualRunnableScheduledFuture<V> implements RunnableScheduledFuture<V> {
        private final RunnableScheduledFuture<V> runnable;
        private final Object context;
        private final ContextHandler<Object> contextHandler;

        public ContextualRunnableScheduledFuture(ContextHandler<Object> contextHandler, Object context,
                RunnableScheduledFuture<V> runnable) {
            this.contextHandler = contextHandler;
            this.context = context;
            this.runnable = runnable;
        }

        @Override
        public boolean isPeriodic() {
            return runnable.isPeriodic();
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return runnable.getDelay(unit);
        }

        @Override
        public int compareTo(Delayed o) {
            return runnable.compareTo(o);
        }

        @Override
        public void run() {
            contextHandler.runWith(runnable, context);
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return runnable.cancel(mayInterruptIfRunning);
        }

        @Override
        public boolean isCancelled() {
            return runnable.isCancelled();
        }

        @Override
        public boolean isDone() {
            return runnable.isDone();
        }

        @Override
        public V get() throws InterruptedException, ExecutionException {
            return runnable.get();
        }

        @Override
        public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return runnable.get(timeout, unit);
        }
    }
}
