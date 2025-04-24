package se.irori.kroxylicious.filter.storage;

import org.apache.kafka.common.record.Record;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static java.util.Objects.requireNonNull;

public abstract class AbstractOversizeStorage implements OversizeValueStorage {

    protected static String getValueAsString(Record record) {

        requireNonNull(record, "record is null");
        requireNonNull(record.value(), "record.value() is null");

        ByteBuffer copy = record.value().asReadOnlyBuffer();
        byte[] bytes = new byte[copy.remaining()];
        copy.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);

    }

}
