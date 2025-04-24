package se.irori.kroxylicious.filter.storage;

import org.apache.kafka.common.record.Record;

import java.util.Optional;

public interface OversizeValueStorage {

    Optional<OversizeValueReference> store(Record record);

    Optional<String> read(OversizeValueReference oversizeValueReference);

}

