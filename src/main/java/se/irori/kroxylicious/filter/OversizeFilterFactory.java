package se.irori.kroxylicious.filter;

import lombok.extern.log4j.Log4j2;
import se.irori.kroxylicious.filter.storage.OversizeValueStorage;
import se.irori.kroxylicious.filter.storage.StorageType;
import se.irori.kroxylicious.filter.storage.TempFileOversizeStorage;

import static se.irori.kroxylicious.filter.storage.StorageType.LOCAL_TEMP_FILE;

@Log4j2
public abstract class OversizeFilterFactory {

    protected final OversizeValueStorage createStorageFromConfig(OversizeFilterConfig config) {

        StorageType storageType = config.storageType();
        log.info("StorageType: {}", storageType);

        if (storageType == null) {
            log.error("StorageType not configured");
            throw new OversizeFilterConfigException();
        }

        if (storageType == LOCAL_TEMP_FILE) {
            return new TempFileOversizeStorage();
        }

        log.error("Unsupported StorageType: {}", storageType);
        throw new OversizeFilterConfigException();
    }

}
