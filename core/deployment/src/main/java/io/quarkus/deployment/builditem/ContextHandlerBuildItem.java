package io.quarkus.deployment.builditem;

import io.quarkus.builder.item.SimpleBuildItem;
import io.quarkus.runtime.configuration.QuarkusContextHandler;

public final class ContextHandlerBuildItem extends SimpleBuildItem {
    private final QuarkusContextHandler<Object> contextHandler;

    public ContextHandlerBuildItem(QuarkusContextHandler<Object> contextHandler) {
        this.contextHandler = contextHandler;
    }

    public QuarkusContextHandler<Object> contextHandler() {
        return contextHandler;
    }
}
