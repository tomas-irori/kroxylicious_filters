package se.irori.kroxylicious.filter;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.ProduceRequestFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.record.*;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.utils.ByteBufferOutputStream;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

@Log4j2
public class OversizeMessageFilter implements ProduceRequestFilter {

    private static final int maxMessageLength = 1024; //TODO make configurable

    @Override
    public CompletionStage<RequestFilterResult> onProduceRequest(
            final short apiVersion,
            RequestHeaderData requestHeaderData,
            ProduceRequestData produceRequestData,
            FilterContext filterContext) {

        try {
            produceRequestData.topicData()
                    .forEach(topicData -> {
                        for (ProduceRequestData.PartitionProduceData partitionData : topicData.partitionData()) {
                            ByteBuffer byteBuffer = getByteBuffer(partitionData.records());
                            if (byteBuffer != null) {
                                MemoryRecordsBuilder builder = createMemoryRecordsBuilder();
                                for (RecordBatch recordBatch : MemoryRecords.readableRecords(byteBuffer).batches()) {
                                    for (Record record : recordBatch) {
                                        processRecord(record, builder);
                                    }
                                }

                                MemoryRecords modifiedRecords = builder.build();
                                partitionData.setRecords(modifiedRecords);
                            }

                        }
                    });
        } catch (Exception e) {
            log.error("{}: Message: {}", getClass().getName(), e.getMessage(), e);
        }

        return filterContext.forwardRequest(requestHeaderData, produceRequestData);
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

    private void processRecord(Record record, MemoryRecordsBuilder builder) {

        try {

            final String keyStr = getString(record.key()); //TODO are these double conversions needed if we don't need to log?
            final ByteBuffer keyByteBuffer = ByteBuffer.wrap(keyStr.getBytes(StandardCharsets.UTF_8));
            log.info("keyStr: {}", keyStr); //TODO remove, don't log sensitive data

            final String valueStr = getString(record.value());
            log.info("valueStr: {}", valueStr); //TODO remove, don't log sensitive data

            if (record.value().remaining() < maxMessageLength) {
                builder.append(record);
                return;
            }

            final Optional<String> optReference = persistMessageValue(valueStr);
            if (optReference.isEmpty()) {
                //TODO what to do if optReference is empty?
                throw new RuntimeException("optReference is empty");
            }
            final String reference = optReference.get();

            Header[] headers = new Header[record.headers().length + 1];
            System.arraycopy(record.headers(), 0, headers, 0, record.headers().length);

            headers[record.headers().length] = new Header() {
                @Override
                public String key() {
                    return "oversize-reference";
                }

                @Override
                public byte[] value() {
                    return reference.getBytes();
                }
            };


            builder.append(
                    new SimpleRecord(
                            record.timestamp(),
                            keyByteBuffer,
                            null,
                            headers));

        } catch (Exception e) {
            log.error("{}", e.getMessage(), e);
            throw new RuntimeException("Processing of record failed: " + e.getMessage());
        }

    }

    private Optional<String> persistMessageValue(final String value) {
        //TODO move to its own class

        try {
            File file = File.createTempFile(getClass().getName(), ".data");
            Files.writeString(Path.of(file.getAbsolutePath()), value);
            log.info("Wrote file: {}", file.getAbsolutePath());
            return Optional.of(file.getAbsolutePath());
        } catch (IOException e) {
            log.error("Failed to create file: {}", e.getMessage(), e);
            return Optional.empty();
        }

    }

    private static ByteBuffer getByteBuffer(BaseRecords baseRecords) {
        ByteBuffer byteBuffer = null;
        if (baseRecords instanceof MemoryRecords) {
            MemoryRecords memoryRecords = (MemoryRecords) baseRecords;
            byteBuffer = memoryRecords.buffer().duplicate(); // duplicate to avoid modifying original
        } else {
            log.error("Unsupported record type: {}", baseRecords.getClass().getName());
        }
        return byteBuffer;
    }

    private static String getString(ByteBuffer byteBuffer) {
        String s = null;
        if (byteBuffer != null) {
            byte[] bytes = new byte[byteBuffer.remaining()];
            byteBuffer.get(bytes);
            // Reset position if you want to re-read
            byteBuffer.rewind();
            s = new String(bytes, StandardCharsets.UTF_8);
        }
        return s;
    }

}
