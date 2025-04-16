package se.irori.kroxylicious.filter;

import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class RecordBatchStreamer implements Iterator<Record> {

    private final Iterator<? extends RecordBatch> batchIterator;
    private Iterator<Record> recordIterator;

    public RecordBatchStreamer(MemoryRecords memoryRecords) {
        this.batchIterator = memoryRecords.batches().iterator();
    }

    @Override
    public boolean hasNext() {
        while ((recordIterator == null || !recordIterator.hasNext()) && batchIterator.hasNext()) {
            recordIterator = batchIterator.next().iterator();
        }
        return recordIterator != null && recordIterator.hasNext();
    }

    @Override
    public Record next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return recordIterator.next();
    }
}
