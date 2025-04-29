package se.irori.kroxylicious.filter.storage;

import org.apache.kafka.common.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class AWSS3OversizeStorage extends AbstractOversizeStorage {

    private static final Logger log = LoggerFactory.getLogger(AWSS3OversizeStorage.class);

    protected static final String BASE_URL_KEY = "BASE_URL";
    protected static final String REGION_KEY = "REGION";
    protected static final String BUCKET_NAME_KEY = "BUCKET_NAME";
    protected static final String S3_ACCESS_KEY_KEY = "S3_ACCESS_KEY";
    protected static final String S3_SECRET_KEY_KEY = "S3_SECRET_KEY";

    private static final String LOCALSTACK_URL = "http://localhost:4566";

    private final S3Client s3Client;

    private final boolean isLocalStack;
    private final String bucketName;
    private final String bucketUrl;


    public AWSS3OversizeStorage(Map<String, String> properties) {

        final String baseUrl = getProperty(properties, BASE_URL_KEY);
        final Region region = Region.of(getProperty(properties, REGION_KEY));
        this.bucketName = getProperty(properties, BUCKET_NAME_KEY);
        final String accessKey = getProperty(properties, S3_ACCESS_KEY_KEY);
        final String secretKey = getProperty(properties, S3_SECRET_KEY_KEY);
        this.isLocalStack = baseUrl.equals(LOCALSTACK_URL);
        this.bucketUrl = isLocalStack ?
                format("%s/%s", LOCALSTACK_URL, bucketName) :
                format("https://%s.s3.%s.amazonaws.com", bucketName, region);

        this.s3Client = S3Client.builder()
                .endpointOverride(URI.create(baseUrl))
                .region(region)
                .credentialsProvider(
                        StaticCredentialsProvider.create(
                                AwsBasicCredentials.create(accessKey, secretKey)
                        )
                )
                .forcePathStyle(isLocalStack)
                .serviceConfiguration(getS3Configuration())
                .build();

    }

    @SuppressWarnings("java:S1874")
    private S3Configuration getS3Configuration() {
        // needed due to a bug in Localstack
        return S3Configuration.builder()
                .checksumValidationEnabled(!isLocalStack)
                .build();
    }

    private static String getProperty(Map<String, String> properties, String key) {
        requireNonNull(properties.get(key), format("Environment variable %s not set", key));
        return properties.get(key);
    }

    @Override
    public Optional<OversizeValueReference> store(Record kRecord) {

        try {
            final String key = createKey();

            PutObjectResponse response = getS3Client().putObject(
                    PutObjectRequest.builder()
                            .bucket(bucketName)
                            .key(key)
                            .applyMutation(builder -> {
                                if (isLocalStack) {
                                    builder.checksumAlgorithm((String) null);
                                }
                            })
                            .build(),
                    RequestBody.fromString(getValueAsString(kRecord)));

            if (!response.sdkHttpResponse().isSuccessful()) {
                log.error(
                        "Upload failed, statusCode: {} statusText: {}",
                        response.sdkHttpResponse().statusCode(),
                        response.sdkHttpResponse().statusText());
                return Optional.empty();
            }

            return Optional.of(
                    OversizeValueReference.of(format("%s/%s", bucketUrl, key)));

        } catch (Exception e) {
            log.error("Store failed, error message: {}", e.getMessage(), e);
            return Optional.empty();
        }

    }

    @Override
    public Optional<String> read(OversizeValueReference oversizeValueReference) {

        try {
            final String key = oversizeValueReference.getRef()
                    .substring(oversizeValueReference.getRef()
                            .lastIndexOf('/') + 1);

            return Optional.of(
                    new String(
                            s3Client.getObject(builder -> builder
                                    .bucket(bucketName)
                                    .key(key)).readAllBytes()));
        } catch (Exception e) {
            log.error("Read failed, error message: {}", e.getMessage(), e);
            return Optional.empty();
        }

    }

    @Override
    public StorageType getStorageType() {
        return StorageType.AWS_S3;
    }

    protected S3Client getS3Client() {
        return s3Client;
    }

    protected String createKey() {
        return UUID.randomUUID().toString();
    }

}

