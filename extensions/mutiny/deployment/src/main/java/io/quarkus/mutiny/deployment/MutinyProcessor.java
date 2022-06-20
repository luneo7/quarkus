package io.quarkus.mutiny.deployment;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import org.jboss.threads.ContextHandler;

import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.annotations.ExecutionTime;
import io.quarkus.deployment.annotations.Record;
import io.quarkus.deployment.builditem.ContextHandlerBuildItem;
import io.quarkus.deployment.builditem.ExecutorBuildItem;
import io.quarkus.deployment.builditem.RemovedResourceBuildItem;
import io.quarkus.deployment.builditem.ShutdownContextBuildItem;
import io.quarkus.maven.dependency.ArtifactKey;
import io.quarkus.mutiny.runtime.MutinyInfrastructure;

public class MutinyProcessor {

    @BuildStep
    RemovedResourceBuildItem overrideSubstitutions() {
        return new RemovedResourceBuildItem(ArtifactKey.fromString("io.smallrye.reactive:mutiny"),
                Collections.singleton("io/smallrye/mutiny/infrastructure/Infrastructure.class"));
    }

    @BuildStep
    @Record(ExecutionTime.RUNTIME_INIT)
    public void runtimeInit(ExecutorBuildItem executorBuildItem, MutinyInfrastructure recorder,
                            Optional<ContextHandlerBuildItem> contextHandlerBuildItem, ShutdownContextBuildItem shutdownContext) {
        ExecutorService executor = executorBuildItem.getExecutorProxy();
        ContextHandler<Object> contextHandler = contextHandlerBuildItem.map(ContextHandlerBuildItem::contextHandler).orElse(null);
        recorder.configureMutinyInfrastructure(executor, contextHandler, shutdownContext);
    }

    @BuildStep
    @Record(ExecutionTime.STATIC_INIT)
    public void buildTimeInit(MutinyInfrastructure recorder) {
        recorder.configureDroppedExceptionHandler();
        recorder.configureThreadBlockingChecker();
        recorder.configureOperatorLogger();
    }
}
