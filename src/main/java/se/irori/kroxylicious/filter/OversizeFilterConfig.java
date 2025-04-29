package se.irori.kroxylicious.filter;

import se.irori.kroxylicious.filter.storage.StorageType;

import java.util.Map;

public record OversizeFilterConfig(StorageType storageType, Map<String, String> properties) {
}

