package io.github.mentegy.s3.channels.testutils;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

public abstract class S3IntegrationTest {

    private AWSCredentials credentials = new BasicAWSCredentials(
            System.getenv("ACCESS_KEY"),
            System.getenv("SECRET_KEY"));

    private AmazonS3 amazonS3;
    private String bucket = "test-bucket";

    protected String testBucket() {
        return bucket;
    }

    protected AmazonS3 defaultAmazonS3() {
        if (amazonS3 == null) {
            amazonS3 = createAmazonS3();
            try {
                amazonS3.createBucket(bucket);
            } catch (Exception ignored) {

            }
        }
        return amazonS3;
    }

    protected AmazonS3 createAmazonS3() {
        return AmazonS3ClientBuilder
                .standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(System.getenv("S3_HOST"), "eu-west"))
                .withPathStyleAccessEnabled(true)
                .build();
    }

    protected void verifyFileContentEquality(Path diskFile, String s3Key) throws IOException {
        final Path tmp = Files.createTempFile("tmp", "" + s3Key.hashCode());
        final ObjectMetadata obj = defaultAmazonS3().getObject(new GetObjectRequest(testBucket(), s3Key), tmp.toFile());

        assertArrayEquals(Files.readAllBytes(diskFile), Files.readAllBytes(tmp));

        Files.delete(tmp);
    }

    private final ThreadLocal<Random> rand = ThreadLocal.withInitial(Random::new);
    protected ByteBuffer shuffleBuffer(ByteBuffer bf) {
        bf.rewind();
        rand.get().nextBytes(bf.array());
        return bf;
    }
}
