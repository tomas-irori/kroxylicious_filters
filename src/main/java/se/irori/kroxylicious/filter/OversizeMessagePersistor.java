package se.irori.kroxylicious.filter;

import java.util.Optional;

public interface OversizeMessagePersistor {
    Optional<OversizeReference> persistValue( org.apache.kafka.common.record.Record record);
}

