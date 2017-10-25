package io.github.mentegy.s3.channels.testutils;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

public abstract class S3IntegrationTest {

    private AWSCredentials credentials = new BasicAWSCredentials(
            System.getenv("ACCESS_KEY"),
            System.getenv("SECRET_KEY"));

    private AmazonS3 amazonS3;
    private String bucket = "test-bucket";

    public String getTestBucket() {
        return bucket;
    }

    public AmazonS3 getDefaultAmazonS3() {
        if (amazonS3 == null) {
            amazonS3 = createAmazonS3();
            try {
                amazonS3.createBucket(bucket);
            } catch (Exception ignored) {

            }
        }
        return amazonS3;
    }

    public AmazonS3 createAmazonS3() {
        return AmazonS3ClientBuilder
                .standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(System.getenv("S3_HOST"), "eu-west"))
                .withPathStyleAccessEnabled(true)
                .build();
    }
}
