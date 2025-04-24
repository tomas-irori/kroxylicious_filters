package se.irori.kroxylicious.filter.storage;

import lombok.extern.log4j.Log4j2;
import org.apache.kafka.common.record.Record;

import java.io.File;
import java.nio.file.Files;
import java.util.Optional;

@Log4j2
public class TempFileOversizeStorage extends AbstractOversizeStorage {

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
                    Files.readString(file.toPath()));
        } catch (Exception e) {
            log.error("Error reading file: {}", oversizeValueReference.getRef());
            return Optional.empty();
        }

    }

}

