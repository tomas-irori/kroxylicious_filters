package se.irori.kroxylicious.filter.persist;

import lombok.extern.log4j.Log4j2;
import org.apache.kafka.common.record.Record;
import se.irori.kroxylicious.filter.OversizeReference;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

@Log4j2
public class TempFilePersistor extends AbstractPersistor {

    @Override
    public Optional<OversizeReference> storeValue(Record record) {

        try {
            File file = File.createTempFile("oversize", ".data");
            Files.writeString(file.toPath(), getValueAsString(record));
            log.info("Persisted oversize message to {}", file.getAbsolutePath());
            return Optional.of(
                    OversizeReference.of(file.getAbsolutePath()));
        } catch (Exception e) {
            log.error("Persistence failed: {}", e.getMessage(), e);
            return Optional.empty();
        }
    }

    @Override
    public String readValue(OversizeReference oversizeReference) {

        File file = new File(oversizeReference.getRef());
        if (!file.exists() || !file.isFile()) {
            log.error("Invalid reference: {}", oversizeReference.getRef());
            throw new RuntimeException(); //TODO more specific exception?
        }
        try {
            return Files.readString(file.toPath());
        } catch (Exception e) {
            log.error("Error reading file: {}", oversizeReference.getRef());
            throw new RuntimeException(e);
        }

    }

}

