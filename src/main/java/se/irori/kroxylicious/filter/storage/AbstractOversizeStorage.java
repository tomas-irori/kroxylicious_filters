package se.irori.kroxylicious.filter.storage;

import org.apache.kafka.common.record.Record;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static java.util.Objects.requireNonNull;

public abstract class AbstractOversizeStorage implements OversizeValueStorage {

    protected AbstractOversizeStorage() {
    }

    protected static String getValueAsString(Record kRecord) {

        requireNonNull(kRecord, "kRecord is null");
        requireNonNull(kRecord.value(), "kRecord.value() is null");

        ByteBuffer copy = kRecord.value().asReadOnlyBuffer();
        byte[] bytes = new byte[copy.remaining()];
        copy.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);

    }

}
