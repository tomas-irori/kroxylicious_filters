package se.irori.kroxylicious.filter;

import se.irori.kroxylicious.filter.persist.OversizePersistor;

public record OversizeFilterConfig(OversizePersistor.Type persistorType) {
}

