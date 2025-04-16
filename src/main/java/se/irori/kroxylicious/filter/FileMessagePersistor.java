package se.irori.kroxylicious.filter;

import lombok.extern.log4j.Log4j2;
import org.apache.kafka.common.record.Record;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

@Log4j2
public class FileMessagePersistor implements OversizeMessagePersistor {

    @Override
    public Optional<OversizeReference> persistValue(Record record) {

        requireNonNull(record, "record is null");

        try {
            ByteBuffer copy = record.value().asReadOnlyBuffer();
            byte[] bytes = new byte[copy.remaining()];
            copy.get(bytes);

            File file = File.createTempFile("oversize", ".data");
            Files.writeString(file.toPath(), new String(bytes, StandardCharsets.UTF_8));
            log.info("Persisted oversize message to {}", file.getAbsolutePath());
            return Optional.of(
                    OversizeReference.of(file.getAbsolutePath()));
        } catch (IOException e) {
            log.error("Persistence failed: {}", e.getMessage(), e);
            return Optional.empty();
        }
    }

}

