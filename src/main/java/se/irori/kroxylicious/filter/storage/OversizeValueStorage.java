package se.irori.kroxylicious.filter.storage;

import org.apache.kafka.common.record.Record;

import java.util.Optional;

public interface OversizeValueStorage {

    StorageType getStorageType();

    Optional<OversizeValueReference> store(Record kRecord);

    Optional<String> read(OversizeValueReference oversizeValueReference);

}

