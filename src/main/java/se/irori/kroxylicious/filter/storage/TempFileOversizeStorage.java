package se.irori.kroxylicious.filter.storage;

import org.apache.kafka.common.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Optional;

public class TempFileOversizeStorage extends AbstractOversizeStorage {

    private static final Logger log = LoggerFactory.getLogger(TempFileOversizeStorage.class);

    @Override
    public Optional<OversizeValueReference> store(Record record) {

        try {
            File file = File.createTempFile("oversize", ".data");
            Files.writeString(file.toPath(), getValueAsString(record));
            log.info("Persisted oversize message to {}", file.getAbsolutePath());
            return Optional.of(
                    OversizeValueReference.of(file.getAbsolutePath()));
        } catch (Exception e) {
            log.error("Persistence failed: {}", e.getMessage(), e);
            return Optional.empty();
        }
    }

    @Override
    public Optional<String> read(OversizeValueReference oversizeValueReference) {

        File file = new File(oversizeValueReference.getRef());
        if (!file.exists() || !file.isFile()) {
            log.error("Invalid reference: {}", oversizeValueReference.getRef());
            throw new RuntimeException(); //TODO more specific exception?
        }
        try {
            return Optional.of(
                    Files.readString(file.toPath(), StandardCharsets.UTF_8));
        } catch (Exception e) {
            log.error("Error reading file: {}", oversizeValueReference.getRef());
            return Optional.empty();
        }

    }

    @Override
    public StorageType getStorageType() {
        return StorageType.LOCAL_TEMP_FILE;
    }

}

