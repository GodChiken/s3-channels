package io.github.mentegy.s3.channels.testutils;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

public abstract class AbstractS3Suite {

    protected String testBucket = "test-bucket";
    private AWSCredentials credentials = new BasicAWSCredentials(env("ACCESS_KEY"), env("SECRET_KEY"));
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

    protected AmazonS3 createAmazonS3() {
        return AmazonS3ClientBuilder
                .standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withEndpointConfiguration(new AwsClientBuilder
                        .EndpointConfiguration(env("S3_HOST"), "eu-west"))
                .withPathStyleAccessEnabled(true)
                .build();
    }

    protected static String env(String key) {
        return System.getenv(key);
    }

}
