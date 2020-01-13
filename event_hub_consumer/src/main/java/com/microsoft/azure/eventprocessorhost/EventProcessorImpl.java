package com.microsoft.azure.eventprocessorhost;

import com.microsoft.azure.eventhubs.EventData;

import java.time.Instant;
import java.util.Map;
import java.util.Iterator;
import java.util.Queue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventProcessorImpl implements IEventProcessor {

    Boolean metadata;
    Queue queue;
    long checkpointInterval;
    long lastCheckpoint;
    static long totalEvents = 0;
    static long totalBytesProcessed = 0;
    Decoder decoder;
    Logger LOGGER = LoggerFactory.getLogger(IEventProcessor.class);

    public EventProcessorImpl(boolean metadata, Queue queue, long checkpointInterval, Decoder decoder){
        this.metadata = metadata;
        this.queue = queue;
        this.checkpointInterval = checkpointInterval;
        this.decoder = decoder;
    }

    public static Long getEventsHandled(){
        return totalEvents;
    }

    public static Long getBytesProcessed() { return totalBytesProcessed; }

    static void incrementEventsHandled() { totalEvents ++; }

    static void incrementBytesProcessed(long bytesProcessed) { totalBytesProcessed += bytesProcessed; }

    @Override
    public void onEvents(PartitionContext context, Iterable batch) throws Exception {
        EventData lastPayload = null;
        int length = 0;
        Iterator iter = batch.iterator();
        do {
            totalEvents++;
            EventData next = (EventData) iter.next();

            byte[] bytes = next.getBytes();
            length += bytes.length;
            totalBytesProcessed += length;
//            try {
                Object eventObject = decoder.decode(bytes);
                if (!(eventObject instanceof Map)){
                    LOGGER.warn("unexpected event type received");
                    continue;
                }
                Map event = (Map)eventObject;
                if (metadata) {
                    event.put("[@metadata][azure_event_hubs][name]", context.getEventHubPath());
                    event.put("[@metadata][azure_event_hubs][consumer_group]", context.getConsumerGroupName());
                    event.put("[@metadata][azure_event_hubs][processor_host]", context.getOwner());
                    event.put("[@metadata][azure_event_hubs][partition]", context.getPartitionId());
                    event.put("[@metadata][azure_event_hubs][offset]", next.getSystemProperties().getOffset());
                    event.put("[@metadata][azure_event_hubs][sequence]", next.getSystemProperties().getSequenceNumber());
                    event.put("[@metadata][azure_event_hubs][timestamp]", next.getSystemProperties().getEnqueuedTime().getEpochSecond());
                    event.put("[@metadata][azure_event_hubs][event_size]", length);
                }
                queue.add(event);
                if (checkpointInterval > 0) {
                    long thisTime = System.currentTimeMillis();
                    long sinceLastCheckpoint = thisTime - lastCheckpoint;
                    if (sinceLastCheckpoint > checkpointInterval) {
                        LOGGER.info("Checkpointing...");
                        context.checkpoint(next);
                    }
                }
                context.checkpoint(lastPayload);
                lastPayload = next;
//            } catch (Exception e) {
//                LOGGER.warn("Unable to process message " + e.getMessage());
//            }
        } while (iter.hasNext());
    }

    @Override
    public void onOpen(PartitionContext context) throws Exception {
        LOGGER.debug("Partition is opening");
    }

    @Override
    public void onClose(PartitionContext context, CloseReason reason) throws Exception {
        LOGGER.debug("Partition is closing");
    }

    @Override
    public void onError(PartitionContext context, Throwable error) {
        LOGGER.error("Partition error");
    }

}
