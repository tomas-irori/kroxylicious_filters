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
import se.irori.kroxylicious.filter.storage.OversizeValueReference;
import se.irori.kroxylicious.filter.storage.OversizeValueStorage;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
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
            final long requestSize = getRequestSize(produceRequestData);
            log.error("requestSize: {}", requestSize);
            //TODO abort if requestSize too large?

            produceRequestData.topicData()
                    .forEach(topicData -> {
                        for (ProduceRequestData.PartitionProduceData partitionData : topicData.partitionData()) {

                            BaseRecords records = partitionData.records();
                            if (!(records instanceof MemoryRecords)) {
                                log.warn("Unsupported record type: {}", records.getClass().getName());
                                continue;
                            }

                            MemoryRecords memoryRecords = (MemoryRecords) records;
                            RecordBatchStreamer streamer = new RecordBatchStreamer(memoryRecords);

                            boolean hasOversize = false;
                            List<MemoryRecords> chunks = new ArrayList<>();
                            MemoryRecordsBuilder builder = null;

                            while (streamer.hasNext()) {

                                Record record = streamer.next();
                                requireNonNull(record, "record is null");

                                if (isTooLargeRecord(record)) {
                                    hasOversize = true;

                                    ByteBuffer keyByteBuffer = record.key() == null ? null : record.key().duplicate();

                                    Optional<OversizeValueReference> optRef = oversizeValueStorage.store(record);
                                    if (optRef.isEmpty()) {
                                        throw new RuntimeException("Failed to persist oversize message");
                                    }
                                    final OversizeValueReference oversizeValueReference = optRef.get();

                                    Header[] newHeaders = new Header[record.headers().length + 1];
                                    arraycopy(record.headers(), 0, newHeaders, 0, record.headers().length);
                                    newHeaders[record.headers().length] = createReferenceHeader(oversizeValueReference);

                                    if (builder == null) {
                                        builder = createMemoryRecordsBuilder(); //TODO need to close builder??
                                    }

                                    builder.append(
                                            new SimpleRecord(
                                                    record.timestamp(),
                                                    keyByteBuffer,
                                                    ByteBuffer.allocate(0),
                                                    newHeaders));
                                } else {
                                    if (hasOversize) {
                                        builder.append(record);
                                    }
                                }

                            }

                            if (hasOversize) {
                                requireNonNull(builder, "MemoryRecordsBuilder must not be null");
                                partitionData.setRecords(builder.build());
                            }

                        }
                    });

        } catch (Exception e) {
            log.error("{}: Message: {}", getClass().getName(), e.getMessage(), e);
            throw new RuntimeException("Processing of messages failed");
        }

        return filterContext.forwardRequest(requestHeaderData, produceRequestData);
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

    private static boolean isTooLargeRecord(Record record) {
        return record.value() != null &&
                record.value().remaining() > maxMessageLength;
    }

    private long getRequestSize(ProduceRequestData produceRequestData) {

        long requestSize = 0L;
        for (ProduceRequestData.TopicProduceData topicData : produceRequestData.topicData()) {
            for (ProduceRequestData.PartitionProduceData partitionData : topicData.partitionData()) {
                ByteBuffer byteBuffer = getByteBuffer(partitionData.records());
                if (byteBuffer == null) {
                    continue;
                }
                for (RecordBatch recordBatch : MemoryRecords.readableRecords(byteBuffer).batches()) {
                    for (Record record : recordBatch) {
                        requestSize += getRecordSize(record);
                    }
                }
            }
        }
        return requestSize;
    }

    private long getRecordSize(Record record) {

        long recordSize = record.key().remaining();
        recordSize += record.value().remaining();
        for (Header header : record.headers()) {
            recordSize += header.key().length();
            recordSize += header.value().length;
        }
        return recordSize;
    }

    private static MemoryRecordsBuilder createMemoryRecordsBuilder() {

        final int bufferSize = 1024 * 1024;

        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(
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
        return builder;
    }

    private static ByteBuffer getByteBuffer(BaseRecords baseRecords) {

        requireNonNull(baseRecords, "baseRecords is null");

        ByteBuffer byteBuffer = null;
        if (baseRecords instanceof MemoryRecords) {
            MemoryRecords memoryRecords = (MemoryRecords) baseRecords;
            byteBuffer = memoryRecords.buffer().duplicate(); // duplicate to avoid modifying original
        } else {
            log.error("Unsupported record type: {}", baseRecords.getClass().getName());
        }
        return byteBuffer;
    }

}
