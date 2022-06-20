package io.quarkus.runtime.configuration;

import java.util.concurrent.Callable;

import org.jboss.threads.ContextHandler;

public interface QuarkusContextHandler<T> extends ContextHandler<T> {
    <V> V callWith(Callable<V> task, T context) throws Exception;
}
