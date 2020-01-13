package com.microsoft.azure.eventprocessorhost;
// ^^ need to be in this package to get access to the hostcontext needed for the in-memory checkpointer

import com.microsoft.azure.eventhubs.ConnectionStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * Consumes messages, but them on a queue
 */
public class Consumer {

    Logger LOGGER = LoggerFactory.getLogger(Consumer.class);
    boolean stopped = false;
    EventProcessorHost host;
    IEventProcessorFactory processoryFactory;

    public Consumer(String connectionString, InMemoryCheckpointManager checkpointManager, InMemoryLeaseManager leaseManager, Queue q){

            String eventHubName = new ConnectionStringBuilder(connectionString).getEventHubName();
            LOGGER.debug("Consuming events from Event Hub {} ...", eventHubName);

            host = new EventProcessorHost(
                    EventProcessorHost.createHostName("logstash"),
                    eventHubName,
                    "$Default",
                    connectionString,
                    checkpointManager,
                    leaseManager);

            checkpointManager.initialize(host.getHostContext());
            leaseManager.initialize(host.getHostContext());
            LOGGER.debug("Registering host named {}", host.getHostName());
            EventProcessorOptions options = new EventProcessorOptions();
            options.setExceptionNotification((exceptionReceivedEventArgs) -> {
                LOGGER.error("Host {} received general error notification during {}", exceptionReceivedEventArgs.getHostname(),
                                                                                        exceptionReceivedEventArgs.getAction(),
                                                                                        exceptionReceivedEventArgs.getException());
            });
            processoryFactory = new LSEventProcessorFactory(q, new JsonDecoder(), true, 100);
    }

    void start() {
        try {
            host.registerEventProcessorFactory(processoryFactory)
                    .whenComplete((unused, exception) ->
                    {
                        LOGGER.debug("Completed Registration");
                    })
                    .thenAccept((unused) ->  {
                        LOGGER.debug("Processing events");
                        while (!stopped) {
                            try {
                                Thread.sleep(10);
                            } catch (InterruptedException e) {
                            }
                        }
                    })
                    .thenCompose((unused) ->
                    {
                        return host.unregisterEventProcessor();
                    })
                    .exceptionally((exception) ->
                    {
                        LOGGER.error("Failure while unregistering");
                        return null;
                    }).get(); // Wait for everything to finish before exiting! Slow!

            LOGGER.info("Finished");
        } catch (Throwable t) {
            LOGGER.error("Error: ", t);
        }
    }

    boolean isStopped(){
        return stopped;
    }

    void stop(){
        stopped = true;
    }
}
