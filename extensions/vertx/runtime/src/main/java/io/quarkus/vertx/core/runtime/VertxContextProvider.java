package io.quarkus.vertx.core.runtime;

import java.util.Map;

import org.eclipse.microprofile.context.spi.ThreadContextProvider;
import org.eclipse.microprofile.context.spi.ThreadContextSnapshot;

import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.impl.ContextInternal;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.impl.WorkerContext;

public class VertxContextProvider implements ThreadContextProvider {

    @Override
    public ThreadContextSnapshot currentContext(Map<String, String> props) {
        Context propagatedContext = Vertx.currentContext();
        return () -> {
            ContextInternal currentContext = (ContextInternal) Vertx.currentContext();
            if (propagatedContext != null && !propagatedContext.equals(currentContext)) {
                final ContextInternal vertxContext = (ContextInternal) propagatedContext;
                vertxContext.beginDispatch();
                return () -> vertxContext.endDispatch(currentContext);
            }
            return () -> {
            };
        };
    }

    @Override
    public ThreadContextSnapshot clearedContext(Map<String, String> props) {
        ContextInternal currentContext = (ContextInternal) Vertx.currentContext();
        if (currentContext != null) {
            return () -> {
                VertxInternal vertx = currentContext.owner();
                WorkerContext workerContext = vertx.createWorkerContext(
                        currentContext.getDeployment(),
                        currentContext.closeFuture(),
                        currentContext.workerPool(),
                        Thread.currentThread().getContextClassLoader());
                workerContext.beginDispatch();
                return () -> workerContext.endDispatch(currentContext);
            };
        }
        return () -> () -> {
        };
    }

    @Override
    public String getThreadContextType() {
        return "Vertx";
    }
}
