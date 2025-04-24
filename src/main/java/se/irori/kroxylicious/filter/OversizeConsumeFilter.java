package se.irori.kroxylicious.filter;

import io.kroxylicious.proxy.filter.FetchResponseFilter;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.record.*;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import se.irori.kroxylicious.filter.storage.OversizeValueReference;
import se.irori.kroxylicious.filter.storage.OversizeValueStorage;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

import static java.util.Objects.requireNonNull;

@Log4j2
public class OversizeConsumeFilter implements FetchResponseFilter {

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

                    MemoryRecords memoryRecords = (MemoryRecords) partition.records();
                    RecordBatchStreamer streamer = new RecordBatchStreamer(memoryRecords);

                    MemoryRecordsBuilder builder = null;
                    boolean hasOversize = false;

                    while (streamer.hasNext()) {
                        Record record = streamer.next();
                        requireNonNull(record, "record is null");

                        ByteBuffer keyByteBuffer = record.key() == null ? null : record.key().duplicate();

                        Optional<OversizeValueReference> optRef = extractReference(record.headers());
                        Optional<String> optValue = Optional.empty();
                        if (optRef.isPresent()) {
                            optValue = oversizeValueStorage.read(optRef.get());
                            if (optValue.isEmpty()) {
                                log.error("Failed to read value from storage, reference: {}", optRef.get());
                            }
                        }

                        if (optValue.isPresent()) {
                            hasOversize = true;
                            Headers newHeaders = new RecordHeaders(record.headers());
                            newHeaders.remove(OversizeValueReference.HEADER_KEY);

                            if (builder == null) {
                                builder = createMemoryRecordsBuilder(); //TODO need to close builder??
                            }

                            //builder.append(newRecord);
                            builder.appendWithOffset(
                                    record.offset(),
                                    new SimpleRecord(
                                            record.timestamp(),
                                            keyByteBuffer,
                                            ByteBuffer.wrap(optValue.get().getBytes(StandardCharsets.UTF_8)),
                                            newHeaders.toArray())); //TODO why does a append() seem to work on Produce but not here?
                        } else {
                            if (hasOversize) {
                                builder.append(record);
                            }

                        }
                    }

                    if (hasOversize) {
                        MemoryRecords updatedMemoryRecords = builder.build();
                        partition.setRecords(updatedMemoryRecords);
                    }

                }
            }

        } catch (Exception e) {
            log.error("Error processing fetch response: {}", e.getMessage(), e);
            throw new RuntimeException("Fetch filtering failed", e);
        }

        return filterContext.forwardResponse(responseHeaderData, fetchResponseData);
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
