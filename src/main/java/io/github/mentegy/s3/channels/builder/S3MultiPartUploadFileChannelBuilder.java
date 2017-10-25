package io.github.mentegy.s3.channels.builder;

import com.amazonaws.services.s3.AmazonS3;
import io.github.mentegy.s3.channels.S3MultiPartUploadFileChannel;
import io.github.mentegy.s3.channels.impl.S3MPUDelayedHeaderFileChannel;
import io.github.mentegy.s3.channels.impl.S3MPUFileChannel;

import java.util.concurrent.ExecutorService;

public class S3MultiPartUploadFileChannelBuilder {
    private String key;
    private String bucket;
    private String uploadId;
    private int partSize = S3MultiPartUploadFileChannel.MIN_PART_SIZE;
    private AmazonS3 amazonS3;
    private ExecutorService executorService;
    private boolean cancelOnFailureInDedicatedThread = true;
    private boolean delayedHeader = false;


    public S3MultiPartUploadFileChannel build() {
        if (bucket == null) {
            throw new IllegalArgumentException("S3 bucket must be set");
        }
        if (uploadId == null) {
            throw new IllegalArgumentException("Multi-part upload id must be set");
        }
        if (key == null) {
            throw new IllegalArgumentException("Object key must be set");
        }
        if (amazonS3 == null) {
            throw new IllegalArgumentException("Amazon s3 must be set");
        }
        if (executorService == null) {
            throw new IllegalArgumentException("Executor service must be set");
        }

        return delayedHeader ?
                new S3MPUDelayedHeaderFileChannel(key, bucket, uploadId, partSize, amazonS3, executorService, cancelOnFailureInDedicatedThread) :
                new S3MPUFileChannel(key, bucket, uploadId, partSize, amazonS3, executorService, cancelOnFailureInDedicatedThread);
    }

    public S3MultiPartUploadFileChannelBuilder withKey(String key) {
        this.key = key;
        return this;
    }

    public S3MultiPartUploadFileChannelBuilder withBucket(String bucket) {
        this.bucket = bucket;
        return this;
    }

    public S3MultiPartUploadFileChannelBuilder withUploadId(String uploadId) {
        this.uploadId = uploadId;
        return this;
    }

    public S3MultiPartUploadFileChannelBuilder withPartSize(int partSize) {
        this.partSize = partSize;
        return this;
    }

    public S3MultiPartUploadFileChannelBuilder withAmazonS3(AmazonS3 s3) {
        this.amazonS3 = s3;
        return this;
    }

    public S3MultiPartUploadFileChannelBuilder withExecutorService(ExecutorService executor) {
        this.executorService = executor;
        return this;
    }

    public S3MultiPartUploadFileChannelBuilder withBlockingCancelRequestOnCompletionFailure() {
        return setCancelOnFailureInDedicatedThread(false);
    }

    public S3MultiPartUploadFileChannelBuilder withDelayedHeader() {
        this.delayedHeader = true;
        return this;
    }

    public String getKey() {
        return key;
    }

    public String getBucket() {
        return bucket;
    }

    public String getUploadId() {
        return uploadId;
    }

    public int getPartSize() {
        return partSize;
    }

    public AmazonS3 getAmazonS3() {
        return amazonS3;
    }

    public ExecutorService getExecutorService() {
        return executorService;
    }

    public boolean isCancelOnFailureInDedicatedThread() {
        return cancelOnFailureInDedicatedThread;
    }

    public S3MultiPartUploadFileChannelBuilder setCancelOnFailureInDedicatedThread(boolean cancelOnFailureInDedicatedThread) {
        this.cancelOnFailureInDedicatedThread = cancelOnFailureInDedicatedThread;
        return this;
    }

    public boolean hasDelayedHeader() {
        return delayedHeader;
    }

}
