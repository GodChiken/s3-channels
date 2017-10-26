package io.github.mentegy.s3.channels.testutils;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
import io.github.mentegy.s3.channels.S3MultiPartUploadFileChannel;
import io.github.mentegy.s3.channels.builder.S3MultiPartUploadFileChannelBuilder;
import io.github.mentegy.s3.channels.impl.S3MPUDelayedHeaderFileChannel;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

public abstract class S3IntegrationTest {

    private AWSCredentials credentials = new BasicAWSCredentials(
            System.getenv("ACCESS_KEY"),
            System.getenv("SECRET_KEY"));

    private AmazonS3 amazonS3;
    protected String testBucket = "test-bucket";
    protected S3MPUDelayedHeaderFileChannel s3channel;
    protected String key;
    protected FileGenerator.TempFile file;
    protected final int chunkSize = 1000;
    protected final int headerSize = 128;

    protected AmazonS3 defaultAmazonS3() {
        if (amazonS3 == null) {
            amazonS3 = createAmazonS3();
            try {
                amazonS3.createBucket(testBucket);
            } catch (Exception ignored) {

            }
        }
        return amazonS3;
    }

    protected InitiateMultipartUploadResult initMultiPart() {
        defaultAmazonS3().deleteObject(testBucket, this.key);
        return defaultAmazonS3()
                .initiateMultipartUpload(new InitiateMultipartUploadRequest(testBucket, key));
    }


    protected S3MultiPartUploadFileChannelBuilder defaultBuilder(String uploadId) {
        return S3MultiPartUploadFileChannel.builder()
                .withAmazonS3(defaultAmazonS3())
                .withExecutorService(Executors.newFixedThreadPool(4))
                .withBucket(testBucket)
                .withKey(key == null ? "key" : key)
                .withUploadId(uploadId)
                .withClosingExecutorOnFinish();
    }

    protected AmazonS3 createAmazonS3() {
        return AmazonS3ClientBuilder
                .standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(System.getenv("S3_HOST"), "eu-west"))
                .withPathStyleAccessEnabled(true)
                .build();
    }

    protected void verifyFileContentEquality() throws IOException {
        final Path tmp = Files.createTempFile("tmp", "" + key.hashCode());
        defaultAmazonS3().getObject(new GetObjectRequest(testBucket, key), tmp.toFile());

        assertArrayEquals(Files.readAllBytes(file.path), Files.readAllBytes(tmp));

        Files.delete(tmp);
    }

    private final ThreadLocal<Random> rand = ThreadLocal.withInitial(Random::new);
    protected ByteBuffer shuffleBuffer(ByteBuffer bf) {
        bf.rewind();
        rand.get().nextBytes(bf.array());
        return bf;
    }
}
