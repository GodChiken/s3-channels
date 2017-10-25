package io.github.mentegy.s3.channels.impl;

import com.amazonaws.services.s3.AmazonS3;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;

public class S3MPUFileChannel extends S3MPUDelayedHeaderFileChannel {
    public S3MPUFileChannel(String key, String bucket, String uploadId, int partSize, AmazonS3 s3,
                            ExecutorService executor, boolean cancelOnFailureInDedicatedThread) {
        super(key, bucket, uploadId, partSize, s3, executor, cancelOnFailureInDedicatedThread);
        this.id = 1;
        this.header = ByteBuffer.allocate(0);
    }

    @Override
    protected void uploadHeader() {
    }
}
