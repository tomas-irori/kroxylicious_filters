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

import java.net.URI;
import java.util.Optional;
import java.util.UUID;

import static java.lang.String.format;

public class AWSS3OversizeStorage extends AbstractOversizeStorage {

    private static final Logger log = LoggerFactory.getLogger(AWSS3OversizeStorage.class);

    private final S3Client s3Client;

    private static final String LOCALSTACK_URL = "http://localhost:4566";
    private static final String baseUrl = LOCALSTACK_URL; // TODO move to config
    private static final boolean isLocalStack = baseUrl.equals(LOCALSTACK_URL); //

    private static final Region region = Region.US_EAST_1; // TODO move to config
    private static final String BUCKET_NAME = "oversize-storage"; // TODO move to config
    private static final String bucketUrl = getBucketUrl();

    public AWSS3OversizeStorage() {

        this.s3Client = S3Client.builder()
                .endpointOverride(URI.create(baseUrl)) // TODO move to config
                .region(region)
                .credentialsProvider(
                        StaticCredentialsProvider.create(
                                AwsBasicCredentials.create(
                                        "accesskey",
                                        "secretkey") //TODO move to secrets
                        )
                )
                .forcePathStyle(isLocalStack)
                .serviceConfiguration(
                        S3Configuration.builder()
                                .checksumValidationEnabled(!isLocalStack) // enable validation ONLY for real AWS
                                .build()
                )
                .build();

    }

    @Override
    public Optional<OversizeValueReference> store(Record kRecord) {

        try {
            final String key = UUID.randomUUID().toString();

            s3Client.putObject(
                    PutObjectRequest.builder()
                            .bucket(BUCKET_NAME)
                            .key(key)
                            .applyMutation(builder -> {
                                if (isLocalStack) {
                                    builder.checksumAlgorithm((String) null);
                                }
                            })
                            .build(),
                    RequestBody.fromString(getValueAsString(kRecord)));

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
                                    .bucket(BUCKET_NAME)
                                    .key(key)).readAllBytes()));
        } catch (Exception e) {
            log.error("Read failed, error message: {}", e.getMessage(), e);
            return Optional.empty();
        }

    }

    private static String getBucketUrl() {
        return isLocalStack ?
                format("%s/%s", LOCALSTACK_URL, BUCKET_NAME) :
                format("https://%s.s3.%s.amazonaws.com", BUCKET_NAME, region);
    }

    @Override
    public StorageType getStorageType() {
        return StorageType.AWS_S3;
    }


}

