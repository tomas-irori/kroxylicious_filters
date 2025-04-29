package se.irori.kroxylicious.filter.storage;


import org.apache.kafka.common.record.Record;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static se.irori.kroxylicious.filter.storage.AWSS3OversizeStorage.*;

class AWSS3OversizeStorageTest {

    private static final String BASE_URL = "http://example.com";
    private static final String BUCKET_NAME = "test-bucket";
    private static final String REGION_NAME = "eu-west-1";
    private static final String S3_ACCESS_KEY = "test-access-key";
    private static final String S3_SECRET_KEY = "test-secret-key";

    private static final Map<String, String> properties = Map.of(
            BASE_URL_KEY, BASE_URL,
            BUCKET_NAME_KEY, BUCKET_NAME,
            REGION_KEY, REGION_NAME,
            S3_ACCESS_KEY_KEY, S3_ACCESS_KEY,
            S3_SECRET_KEY_KEY, S3_SECRET_KEY
    );

    @Test
    void shouldStore() {

        final String testKey = "test-key";

        S3Client s3Client = mock(S3Client.class);
        PutObjectResponse putObjectResponse = mock(PutObjectResponse.class);
        SdkHttpResponse sdkHttpResponse = mock(SdkHttpResponse.class);
        when(sdkHttpResponse.isSuccessful()).thenReturn(true);
        when(putObjectResponse.sdkHttpResponse()).thenReturn(sdkHttpResponse);
        when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenReturn(putObjectResponse);


        var awsS3OversizeStorage = new AWSS3OversizeStorage(properties) {
            @Override
            protected S3Client getS3Client() {
                return s3Client;
            }

            @Override
            protected String createKey() {
                return testKey;
            }
        };
        Record kRecord = mock(Record.class);
        when(kRecord.value()).thenReturn(ByteBuffer.wrap("foo".getBytes()));

        Optional<OversizeValueReference> optionalOversizeValueReference =
                awsS3OversizeStorage.store(kRecord);

        assertNotNull(optionalOversizeValueReference);
        assertTrue(optionalOversizeValueReference.isPresent());
        assertEquals(
                "https://test-bucket.s3.eu-west-1.amazonaws.com/" + testKey,
                optionalOversizeValueReference.get().getRef());

    }

    @ParameterizedTest
    @ValueSource(strings = {
            BASE_URL_KEY,
            REGION_KEY,
            BUCKET_NAME_KEY,
            S3_ACCESS_KEY_KEY,
            S3_SECRET_KEY_KEY})
    void shouldDoNullChecksInConstructor(final String propertyKey) {

        Map<String, String> testProperties =
                getPropertiesWithNull(propertyKey);

        assertThrows(NullPointerException.class, () -> {
            new AWSS3OversizeStorage(testProperties);
        });

    }

    private static Map<String, String> getPropertiesWithNull(final String keyToMakeNull) {

        Map<String, String> deepClone = new HashMap<>();
        for (Map.Entry<String, String> entry : AWSS3OversizeStorageTest.properties.entrySet()) {
            deepClone.put(
                    entry.getKey(),
                    entry.getKey().equals(keyToMakeNull) ? null : entry.getValue());
        }
        return deepClone;

    }


    @Test
    void shouldReturnAwsS3() {

        assertEquals(
                StorageType.AWS_S3,
                new AWSS3OversizeStorage(properties).getStorageType(),
                "Storage type mismatch");
    }
}