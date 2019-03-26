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
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * Uses EPH, but only the in-memory version... so this only works as a single standalone consumer using the $Default consumer group.
 */
public class Main {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    /**
     * Usage:
     *
     * cd integration/event_hub_consumer
     * mvn package
     * java -jar target/event-hub-consumer.jar "[your_connection_string_here]"
     */
    public static void main(String... args) {

        if (args.length != 1 || !args[0].startsWith("Endpoint=sb") || !args[0].contains("EntityPath")) {
            LOGGER.error("The first and only argument must be the event hub connection string with the EntityPath. For example:");
            LOGGER.error("Endpoint=sb://logstash-demo.servicebus.windows.net/;SharedAccessKeyName=activity-log-ro;SharedAccessKey=<redacted>;EntityPath=my_event_hub");
            System.exit(1);
        }
        ArrayBlockingQueue q = new ArrayBlockingQueue(100);
        Consumer consumer = new Consumer(args[0], new InMemoryCheckpointManager(), new InMemoryLeaseManager(), q);
        Thread keyboard = new Thread(() -> {
            try {
                System.out.println("Press any key");
                System.in.read();
            }
            catch (Exception e){
                    e.printStackTrace();
            }finally{
                consumer.stop();
                LOGGER.info("stopping: The total number of events handled is {}. number of bytes processed is {}",
                        EventProcessorImpl.getEventsHandled(),
                        EventProcessorImpl.getBytesProcessed());
            }
        });
        Thread poller = new Thread(() -> {
            do
                try{
                    Object nextItem = q.poll(10, TimeUnit.SECONDS);
                    System.out.println("The next item is "  + nextItem);
                }catch (Exception e) {
                    e.printStackTrace();
                }
            while(!consumer.isStopped());
        });
        poller.start();
        keyboard.start();
        consumer.start();
    }
}
