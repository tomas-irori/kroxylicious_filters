package se.irori.kroxylicious.filter;

import io.kroxylicious.proxy.filter.FetchResponseFilter;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.record.*;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.irori.kroxylicious.filter.exception.FetchResponseProcessingException;
import se.irori.kroxylicious.filter.exception.PartitionProcessingException;
import se.irori.kroxylicious.filter.storage.OversizeValueReference;
import se.irori.kroxylicious.filter.storage.OversizeValueStorage;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

import static java.util.Objects.requireNonNull;

public class OversizeConsumeFilter implements FetchResponseFilter {

    private static final Logger log = LoggerFactory.getLogger(OversizeConsumeFilter.class);

    private final OversizeValueStorage oversizeValueStorage;

    public OversizeConsumeFilter(OversizeValueStorage oversizeValueStorage) {
        this.oversizeValueStorage = oversizeValueStorage;
    }

    @Override
    public boolean shouldHandleFetchResponse(short apiVersion) {
        return true; // You can restrict this based on API version if needed
    }

    @Override
    public CompletionStage<ResponseFilterResult> onFetchResponse(
            short apiVersion,
            ResponseHeaderData responseHeaderData,
            FetchResponseData fetchResponseData,
            FilterContext filterContext) {

        try {
            for (FetchResponseData.FetchableTopicResponse topicResponse : fetchResponseData.responses()) {
                for (FetchResponseData.PartitionData partition : topicResponse.partitions()) {
                    processPartition(partition);
                }
            }
        } catch (RuntimeException e) {
            log.error("Error processing fetch response: {}", e.getMessage(), e);
            throw new FetchResponseProcessingException( e);
        }

        return filterContext.forwardResponse(responseHeaderData, fetchResponseData);
    }

    private void processPartition(FetchResponseData.PartitionData partition) {
        MemoryRecords memoryRecords = (MemoryRecords) partition.records();
        RecordBatchStreamer streamer = new RecordBatchStreamer(memoryRecords);

        boolean hasOversize = false;

        try (MemoryRecordsBuilder builder = createMemoryRecordsBuilder()) {
            while (streamer.hasNext()) {
                Record kRecord = requireNonNull(streamer.next(), "kRecord is null");
                Optional<String> resolvedValue = resolveOversizeValue(kRecord);
                if (resolvedValue.isPresent()) {
                    hasOversize = true;
                    builder.appendWithOffset(
                            kRecord.offset(),
                            createRecordWithResolvedValue(kRecord, resolvedValue.get()));
                } else if (hasOversize) {
                    builder.append(kRecord);
                }
            }

            if (hasOversize) {
                partition.setRecords(builder.build());
            }
        } catch (RuntimeException e) {
            log.error("Error processing partition: {}", e.getMessage(), e);
            throw new PartitionProcessingException(e);
        }
    }


    private Optional<String> resolveOversizeValue(Record kRecord) {
        return extractReference(kRecord.headers())
                .flatMap(ref -> {
                    Optional<String> value = oversizeValueStorage.read(ref);
                    if (value.isEmpty()) {
                        log.error("Failed to read value from storage, reference: {}", ref);
                    }
                    return value;
                });
    }

    private SimpleRecord createRecordWithResolvedValue(Record original, String newValue) {
        Headers newHeaders = new RecordHeaders(original.headers());
        newHeaders.remove(OversizeValueReference.HEADER_KEY);

        ByteBuffer keyBuffer = original.key() == null ? null : original.key().duplicate();

        return new SimpleRecord(
                original.timestamp(),
                keyBuffer,
                ByteBuffer.wrap(newValue.getBytes(StandardCharsets.UTF_8)),
                newHeaders.toArray()
        );
    }


    private Optional<OversizeValueReference> extractReference(Header[] headers) {

        for (Header header : headers) {
            if (OversizeValueReference.HEADER_KEY.equals(header.key())) {
                return Optional.of(
                        OversizeValueReference.of(
                                new String(header.value(), StandardCharsets.UTF_8)));
            }
        }
        return Optional.empty();

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
