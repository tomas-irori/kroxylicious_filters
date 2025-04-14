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
import java.util.concurrent.CompletionStage;

@Log4j2
public class MyLoggingProduceFilter implements ProduceRequestFilter {

    @Override
    public CompletionStage<RequestFilterResult> onProduceRequest(
            final short apiVersion,
            RequestHeaderData header,
            ProduceRequestData request,
            FilterContext context) {

        request.topicData().forEach(topicData -> {
            for (ProduceRequestData.PartitionProduceData partitionData : topicData.partitionData()) {
                BaseRecords baseRecords = partitionData.records();

                ByteBuffer byteBuffer = null;
                if (baseRecords instanceof MemoryRecords) {
                    MemoryRecords memoryRecords = (MemoryRecords) baseRecords;
                     byteBuffer = memoryRecords.buffer().duplicate(); // duplicate to avoid modifying original
                    // Now you can read or inspect the message bytes
                } else {
                    // Handle other record types if necessary
                    log.error("Unsupported record type: {}", baseRecords.getClass().getName());
                }

                if (byteBuffer != null) {
                    MemoryRecords memoryRecords = MemoryRecords.readableRecords(byteBuffer);

                    for (RecordBatch batch : memoryRecords.batches()) {
                        for (Record record : batch) {
                            // Access key/value
                            ByteBuffer key = record.key();
                            ByteBuffer value = record.value();

                            String keyStr = key == null ? null : new String(key.array());
                            String valueStr = value == null ? null : new String(value.array());

                            log.info("kroxylicious, key: {} value: {}", keyStr,valueStr);
                            System.out.println("kroxylicious Key: " + keyStr);
                            System.out.println("kroxylicious Value: " + valueStr);

                            // You can now inspect/modify the record bytes — but to actually *change* the message,
                            // you'll need to construct new records and replace the ByteBuffer in `partitionData`
                            // (which is a bit trickier).
                        }
                    }
                }
            }
        });

        return context.forwardRequest(header, request);
    }


}
