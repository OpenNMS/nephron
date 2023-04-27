package org.opennms.nephron.v2;

import org.opennms.netmgt.flows.persistence.model.FlowDocument;

import java.util.concurrent.BlockingQueue;

public interface PipelineRunner {

    void run(String[] args, BlockingQueue<FlowDocument> queue);

}
