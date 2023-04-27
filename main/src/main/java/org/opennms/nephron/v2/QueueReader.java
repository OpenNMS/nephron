package org.opennms.nephron.v2;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.joda.time.Instant;
import org.opennms.nephron.coders.FlowDocumentProtobufCoder;
import org.opennms.netmgt.flows.persistence.model.FlowDocument;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class QueueReader extends UnboundedSource<FlowDocument, UnboundedSource.CheckpointMark.NoopCheckpointMark> {

    private final Queue<FlowDocument> flows;

    private final AtomicReference<FlowDocument> currentFlow = new AtomicReference<>();

    public QueueReader(Queue<FlowDocument> flows) {
        this.flows = Objects.requireNonNull(flows);
    }

    @Override
    public List<? extends UnboundedSource<FlowDocument, CheckpointMark.NoopCheckpointMark>> split(int desiredNumSplits, PipelineOptions options) {
        // don't split
        return Collections.singletonList(this);
    }

    @Override
    public UnboundedReader<FlowDocument> createReader(PipelineOptions options, CheckpointMark.NoopCheckpointMark checkpointMark)  {
        return new Reader();
    }

    @Override
    public Coder<FlowDocument> getOutputCoder() {
        return new FlowDocumentProtobufCoder();
    }

    private class Reader extends UnboundedReader<FlowDocument> {
        private final CheckpointMark.NoopCheckpointMark checkpointMark = new CheckpointMark.NoopCheckpointMark();

        @Override
        public boolean start() {
            return advance();
        }

        @Override
        public boolean advance() {
            FlowDocument flow = flows.poll();
            if (flow != null) {
                currentFlow.set(flow);
                return true;
            }
            return false;
        }

        @Override
        public FlowDocument getCurrent() throws NoSuchElementException {
            return currentFlow.get();
        }

        @Override
        public Instant getCurrentTimestamp() throws NoSuchElementException {
            return Instant.ofEpochMilli(currentFlow.get().getLastSwitched().getValue());
        }

        @Override
        public void close() {
            // noop
        }

        @Override
        public Instant getWatermark() {
            return Instant.now().minus(TimeUnit.MINUTES.toMillis(5));
        }

        @Override
        public CheckpointMark getCheckpointMark() {
            return checkpointMark;
        }

        @Override
        public UnboundedSource<FlowDocument, ?> getCurrentSource() {
            return QueueReader.this;
        }
    };

    @Override
    public Coder<CheckpointMark.NoopCheckpointMark> getCheckpointMarkCoder() {
        return new NoopCoder();
    }

    private static class NoopCoder extends StructuredCoder<CheckpointMark.NoopCheckpointMark> {

        @Override
        public void encode(CheckpointMark.NoopCheckpointMark value, OutputStream outStream) {
            // noop
        }

        @Override
        public CheckpointMark.NoopCheckpointMark decode(InputStream inStream) {
            return new CheckpointMark.NoopCheckpointMark();
        }

        @Override
        public List<? extends Coder<?>> getCoderArguments() {
            return Collections.emptyList();
        }

        @Override
        public void verifyDeterministic() {
            // very deterministic
        }
    };
}
