package com.microsoft.azure.eventprocessorhost;

import java.util.Queue;

public class LSEventProcessorFactory implements IEventProcessorFactory {
    private Queue q;
    private Decoder dec;
    private boolean md;
    private long cpI;

    public LSEventProcessorFactory(Queue q, Decoder dec, boolean md, long cpI) {
        this.q = q;
        this.dec = dec;
        this.md = md;
        this.cpI = cpI;
    }

    public IEventProcessor createEventProcessor(final PartitionContext context) throws Exception {
        return new EventProcessorImpl(this.md, q, cpI, dec);
    }
}
