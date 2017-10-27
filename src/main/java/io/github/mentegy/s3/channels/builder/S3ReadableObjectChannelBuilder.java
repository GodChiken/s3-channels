package io.github.mentegy.s3.channels.builder;

import com.amazonaws.services.s3.AmazonS3;
import io.github.mentegy.s3.channels.S3ReadableObjectChannel;
import io.github.mentegy.s3.channels.impl.S3RangedReadObjectChannel;

public class S3ReadableObjectChannelBuilder {
    private String key;
    private String bucket;
    private AmazonS3 amazonS3;


    public S3ReadableObjectChannel build() {
        if (bucket == null) {
            throw new IllegalArgumentException("S3 bucket must be set");
        }
        if (key == null) {
            throw new IllegalArgumentException("Object key must be set");
        }
        if (amazonS3 == null) {
            throw new IllegalArgumentException("Amazon s3 must be set");
        }
        return new S3RangedReadObjectChannel(key, bucket, amazonS3);
    }

    /**
     * S3 object key
     */
    public S3ReadableObjectChannelBuilder key(String key) {
        this.key = key;
        return this;
    }

    /**
     * S3 bucket
     */
    public S3ReadableObjectChannelBuilder bucket(String bucket) {
        this.bucket = bucket;
        return this;
    }

    /**
     * Amazon S3 client
     */
    public S3ReadableObjectChannelBuilder amazonS3(AmazonS3 amazonS3) {
        this.amazonS3 = amazonS3;
        return this;
    }


    public String key() {
        return key;
    }

    public String bucket() {
        return bucket;
    }

    public AmazonS3 amazonS3() {
        return amazonS3;
    }
}
