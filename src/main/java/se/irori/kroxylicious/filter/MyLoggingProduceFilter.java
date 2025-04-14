package se.irori.kroxylicious.filter;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.ProduceRequestFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.record.BaseRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.CompletionStage;

@Log4j2
public class MyLoggingProduceFilter implements ProduceRequestFilter {

    @Override
    public CompletionStage<RequestFilterResult> onProduceRequest(
            final short apiVersion,
            RequestHeaderData requestHeaderData,
            ProduceRequestData produceRequestData,
            FilterContext filterContext) {

        try {
            produceRequestData.topicData().forEach(topicData -> {
                if (log.isTraceEnabled()) {
                    log.trace("topicData: {}", topicData.toString());
                }
                for (ProduceRequestData.PartitionProduceData partitionData : topicData.partitionData()) {
                    if (log.isTraceEnabled()) {
                        log.trace("partitionData: {}", partitionData.toString());
                    }
                    BaseRecords baseRecords = partitionData.records();
                    if (log.isTraceEnabled()) {
                        log.trace("baseRecords: {}", baseRecords.toString());
                    }

                    ByteBuffer byteBuffer = null;
                    if (baseRecords instanceof MemoryRecords) {
                        MemoryRecords memoryRecords = (MemoryRecords) baseRecords;
                        byteBuffer = memoryRecords.buffer().duplicate(); // duplicate to avoid modifying original
                        // Now you can read or inspect the message bytes
                        if (log.isTraceEnabled()) {
                            log.trace("byteBuffer: {}", byteBuffer.toString());
                        }
                    } else {
                        // Handle other record types if necessary
                        log.error("Unsupported record type: {}", baseRecords.getClass().getName());
                    }

                    if (byteBuffer != null) {
                        MemoryRecords memoryRecords = MemoryRecords.readableRecords(byteBuffer);
                        if (log.isTraceEnabled()) {
                            log.trace("memoryRecords: {}", memoryRecords);
                        }
                        for (RecordBatch recordBatch : memoryRecords.batches()) {
                            if (log.isTraceEnabled()) {
                                log.trace("recordBatch: {}", recordBatch);
                            }
                            for (Record record : recordBatch) {
                                if (log.isTraceEnabled()) {
                                    log.trace("record: {}", record);
                                }
                                try {
                                    ByteBuffer keyBuffer = record.key();
                                    if (log.isTraceEnabled()) {
                                        log.trace("keyBuffer: {}", keyBuffer);
                                    }
                                    final String keyStr = getString(keyBuffer);
                                    log.info("keyStr: {}", keyStr);


                                    ByteBuffer valueBuffer = record.value();
                                    if (log.isTraceEnabled()) {
                                        log.trace("valueBuffer: {}", valueBuffer);
                                    }
                                    final String valueStr = getString(valueBuffer);
                                    log.info("valueStr: {}", valueStr);

                                    Arrays.stream(record.headers())
                                            .forEach(header ->
                                                    log.info("header, key: {} value: {}", header.key(), header.value()));

                                } catch (Exception e) {
                                    log.error("{}", e.getMessage(), e);
                                }

                                // You can now inspect/modify the record bytes — but to actually *change* the message,
                                // you'll need to construct new records and replace the ByteBuffer in `partitionData`
                                // (which is a bit trickier).
                            }
                        }
                    }


                }
            });
        } catch (Exception e) {
            log.error("{}: Message: {}", getClass().getName(), e.getMessage(), e);
        }

        log.info("exiting");
        return filterContext.forwardRequest(requestHeaderData, produceRequestData);
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
