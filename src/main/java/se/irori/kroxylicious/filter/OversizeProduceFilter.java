package se.irori.kroxylicious.filter;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.ProduceRequestFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.record.*;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.irori.kroxylicious.filter.exception.MessageProcessingException;
import se.irori.kroxylicious.filter.exception.PersistFailedException;
import se.irori.kroxylicious.filter.storage.OversizeValueReference;
import se.irori.kroxylicious.filter.storage.OversizeValueStorage;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletionStage;

import static java.lang.System.arraycopy;
import static java.util.Objects.requireNonNull;

public class OversizeProduceFilter implements ProduceRequestFilter {

    private static final Logger log = LoggerFactory.getLogger(OversizeProduceFilter.class);

    private static final int maxMessageLength = 1024; //TODO make configurable

    private final OversizeValueStorage oversizeValueStorage;

    public OversizeProduceFilter(OversizeValueStorage oversizeValueStorage) {
        this.oversizeValueStorage = oversizeValueStorage;
        log.info("StorageType: {}", oversizeValueStorage.getStorageType());
    }

    @Override
    public CompletionStage<RequestFilterResult> onProduceRequest(
            final short apiVersion,
            RequestHeaderData requestHeaderData,
            ProduceRequestData produceRequestData,
            FilterContext filterContext) {

        try {
            for (ProduceRequestData.TopicProduceData topicData : produceRequestData.topicData()) {
                for (ProduceRequestData.PartitionProduceData partitionData : topicData.partitionData()) {
                    processPartition(partitionData);
                }
            }
        } catch (Exception e) {
            log.error("{}: Message: {}", getClass().getName(), e.getMessage(), e);
            throw new MessageProcessingException();
        }

        return filterContext.forwardRequest(requestHeaderData, produceRequestData);
    }

    private void processPartition(ProduceRequestData.PartitionProduceData partitionData) {
        BaseRecords records = partitionData.records();
        if (!(records instanceof MemoryRecords memoryRecords)) {
            log.warn("Unsupported record type: {}", records.getClass().getName());
            return;
        }

        RecordBatchStreamer streamer = new RecordBatchStreamer(memoryRecords);
        boolean hasOversize = false;

        try (MemoryRecordsBuilder builder = createMemoryRecordsBuilder()) {
            while (streamer.hasNext()) {
                Record kRecord = requireNonNull(streamer.next(), "Record is null");

                if (isTooLargeRecord(kRecord)) {
                    hasOversize = true;
                    builder.append(createReferenceRecord(kRecord));
                } else if (hasOversize) {
                    builder.append(kRecord);
                }
            }

            if (hasOversize) {
                partitionData.setRecords(builder.build());
            }
        } catch (Exception e) {
            log.error("Error processing partition: {}", e.getMessage(), e);
            throw new PersistFailedException();
        }
    }

    private SimpleRecord createReferenceRecord(Record kRecord) {
        ByteBuffer keyCopy = kRecord.key() == null ? null : kRecord.key().duplicate();

        OversizeValueReference reference = oversizeValueStorage.store(kRecord)
                .orElseThrow(PersistFailedException::new);

        Header[] newHeaders = extendHeadersWithReference(kRecord.headers(), reference);

        return new SimpleRecord(
                kRecord.timestamp(),
                keyCopy,
                ByteBuffer.allocate(0), // Replace the value with empty buffer
                newHeaders
        );
    }

    private Header[] extendHeadersWithReference(Header[] originalHeaders, OversizeValueReference reference) {
        Header[] newHeaders = new Header[originalHeaders.length + 1];
        arraycopy(originalHeaders, 0, newHeaders, 0, originalHeaders.length);
        newHeaders[originalHeaders.length] = createReferenceHeader(reference);
        return newHeaders;
    }


    private static Header createReferenceHeader(final OversizeValueReference oversizeValueReference) {

        // @formatter:off
        return new Header() {
            @Override public String key() {
                return OversizeValueReference.HEADER_KEY; }
            @Override public byte[] value() {
                return oversizeValueReference.getRef().getBytes(StandardCharsets.UTF_8); }
        };
        // @formatter:on

    }

    private static boolean isTooLargeRecord(Record kRecord) {
        return kRecord.value() != null &&
                kRecord.value().remaining() > maxMessageLength;
    }

    private static MemoryRecordsBuilder createMemoryRecordsBuilder() {

        final int bufferSize = 1024 * 1024;

        return new MemoryRecordsBuilder(
                new ByteBufferOutputStream(bufferSize),
                RecordBatch.CURRENT_MAGIC_VALUE,
                Compression.NONE,
                TimestampType.CREATE_TIME,
                0L,
                System.currentTimeMillis(),
                RecordBatch.NO_PRODUCER_ID,
                RecordBatch.NO_PRODUCER_EPOCH,
                RecordBatch.NO_SEQUENCE,
                false,
                false,
                0,
                bufferSize,
                -1
        );
    }

}
