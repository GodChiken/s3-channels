package io.github.mentegy.s3.channels.impl;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import io.github.mentegy.s3.channels.S3MultiPartUploadChannel;
import io.github.mentegy.s3.channels.builder.S3MultiPartUploadFileChannelBuilder;
import io.github.mentegy.s3.channels.testutils.FileGenerator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

public abstract class AbstractS3MPUChannelSuite<T extends S3MultiPartUploadChannel> {

    protected final int chunkSize = 1000;
    protected final int headerSize = 128;
    protected String testBucket = "test-bucket";
    protected T s3channel;
    protected String key;
    protected FileGenerator.TempFile file;
    private AWSCredentials credentials = new BasicAWSCredentials(
            System.getenv("ACCESS_KEY"),
            System.getenv("SECRET_KEY"));
    private AmazonS3 amazonS3;

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
        return defaultAmazonS3().initiateMultipartUpload(new InitiateMultipartUploadRequest(testBucket, key));
    }


    protected S3MultiPartUploadFileChannelBuilder defaultBuilder(String uploadId) {
        return S3MultiPartUploadChannel.builder()
                .amazonS3(defaultAmazonS3())
                .defaultExecutorService()
                .bucket(testBucket)
                .key(key == null ? "key" : key)
                .uploadId(uploadId);
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
}
