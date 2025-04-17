package se.irori.kroxylicious.filter.persist;

import se.irori.kroxylicious.filter.OversizeReference;

import java.util.Optional;

public interface OversizePersistor {

    enum Type {
        LOCAL_TEMP_FILE
    }

    Optional<OversizeReference> storeValue(org.apache.kafka.common.record.Record record);

    String readValue(OversizeReference oversizeReference);

}

