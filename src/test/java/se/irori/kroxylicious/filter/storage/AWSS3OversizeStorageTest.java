package se.irori.kroxylicious.filter.storage;

import org.apache.kafka.common.record.Record;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AWSS3OversizeStorageTest {

    private AWSS3OversizeStorage awsS3OversizeStorage;

    @BeforeEach
    void beforeEach(){
        this.awsS3OversizeStorage=new AWSS3OversizeStorage();
    }
    @Test
    void shouldStore() {

        Record record = mock(Record.class);
        when(record.value()).thenReturn(ByteBuffer.wrap("foo1525".getBytes(StandardCharsets.UTF_8)));

        awsS3OversizeStorage.store(record);

    }

    @Test
    void read() {
    }

    @Test
    void getStorageType() {
    }
}