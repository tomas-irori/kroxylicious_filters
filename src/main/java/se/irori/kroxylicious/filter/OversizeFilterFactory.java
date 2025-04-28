package se.irori.kroxylicious.filter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.irori.kroxylicious.filter.exception.OversizeFilterConfigException;
import se.irori.kroxylicious.filter.storage.AWSS3OversizeStorage;
import se.irori.kroxylicious.filter.storage.OversizeValueStorage;
import se.irori.kroxylicious.filter.storage.StorageType;
import se.irori.kroxylicious.filter.storage.TempFileOversizeStorage;

import static se.irori.kroxylicious.filter.storage.StorageType.AWS_S3;
import static se.irori.kroxylicious.filter.storage.StorageType.LOCAL_TEMP_FILE;

public abstract class OversizeFilterFactory {

    private static final Logger log = LoggerFactory.getLogger(OversizeFilterFactory.class);

    protected final OversizeValueStorage createStorageFromConfig(OversizeFilterConfig config) {

        StorageType storageType = config.storageType();

        if (storageType == null) {
            log.error("StorageType not configured");
            throw new OversizeFilterConfigException();
        }

        if (storageType == LOCAL_TEMP_FILE) {
            return new TempFileOversizeStorage();
        } else if (storageType == AWS_S3) {
            return new AWSS3OversizeStorage();
        }
        log.error("Unsupported StorageType: {}", storageType);
        throw new OversizeFilterConfigException();
    }

}
