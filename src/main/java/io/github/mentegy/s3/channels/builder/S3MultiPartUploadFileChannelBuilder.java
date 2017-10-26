package io.github.mentegy.s3.channels.builder;

import com.amazonaws.services.s3.AmazonS3;
import io.github.mentegy.s3.channels.S3MultiPartUploadChannel;
import io.github.mentegy.s3.channels.impl.S3MPUDelayedHeaderChannel;
import io.github.mentegy.s3.channels.impl.S3MPUChannel;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class S3MultiPartUploadFileChannelBuilder {
    private String key;
    private String bucket;
    private String uploadId;
    private int partSize = S3MultiPartUploadChannel.MIN_PART_SIZE;
    private int failedPartUploadRetries;
    private AmazonS3 amazonS3;
    private ExecutorService executorService;
    private boolean delayedHeader = false;
    private boolean closeExecutorOnChannelClose = false;


    public S3MultiPartUploadChannel build() {
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
                new S3MPUDelayedHeaderChannel(key, bucket, uploadId, partSize, amazonS3, executorService, closeExecutorOnChannelClose, failedPartUploadRetries) :
                new S3MPUChannel(key, bucket, uploadId, partSize, amazonS3, executorService, closeExecutorOnChannelClose, failedPartUploadRetries);
    }

    /**
     * S3 object key
     */
    public S3MultiPartUploadFileChannelBuilder key(String key) {
        this.key = key;
        return this;
    }

    /**
     * S3 bucket
     */
    public S3MultiPartUploadFileChannelBuilder bucket(String bucket) {
        this.bucket = bucket;
        return this;
    }

    /**
     * S3 multi-part upload id
     */
    public S3MultiPartUploadFileChannelBuilder uploadId(String uploadId) {
        this.uploadId = uploadId;
        return this;
    }

    /**
     * Average size in bytes of each part being uploaded into S3 via multipart upload.
     * Most S3 implementations does not allow to set this values less than 5MB
     *
     * Default 5MB
     */
    public S3MultiPartUploadFileChannelBuilder partSize(int partSize) {
        this.partSize = partSize;
        return this;
    }

    /**
     * Number of retries t re-upload failed part. If upload of failed part is still failing
     * after reaching this number then whole upload is cancelled.
     *
     * Default 0
     */
    public S3MultiPartUploadFileChannelBuilder failedPartUploadRetries(int failedPartUploadRetries) {
        this.failedPartUploadRetries = failedPartUploadRetries;
        return this;
    }

    /**
     * Amazon S3 client
     */
    public S3MultiPartUploadFileChannelBuilder amazonS3(AmazonS3 amazonS3) {
        this.amazonS3 = amazonS3;
        return this;
    }

    /**
     * Executor service which takes care of uploading parts into S3.
     */
    public S3MultiPartUploadFileChannelBuilder executorService(ExecutorService executorService) {
        this.executorService = executorService;
        return this;
    }

    /**
     * Sets default executor service as FixedThreadPool with 4 threads
     */
    public S3MultiPartUploadFileChannelBuilder defaultExecutorService() {
        closeExecutorOnChannelClose(true);
        this.executorService = Executors.newFixedThreadPool(4);
        return this;
    }

    /**
     * Whereas created instance should be {@link S3MPUDelayedHeaderChannel}
     *
     * Default false
     */
    public S3MultiPartUploadFileChannelBuilder delayedHeader(boolean delayedHeader) {
        this.delayedHeader = delayedHeader;
        return this;
    }

    /**
     * Whereas to close executor service on channel close
     */
    public S3MultiPartUploadFileChannelBuilder closeExecutorOnChannelClose(boolean closeExecutorOnChannelClose) {
        this.closeExecutorOnChannelClose = closeExecutorOnChannelClose;
        return this;
    }

    public String key() {
        return key;
    }

    public String bucket() {
        return bucket;
    }

    public String uploadId() {
        return uploadId;
    }

    public int getPartSize() {
        return partSize;
    }

    public int failedPartUploadRetries() {
        return failedPartUploadRetries;
    }

    public AmazonS3 amazonS3() {
        return amazonS3;
    }

    public ExecutorService executorService() {
        return executorService;
    }

    public boolean hasDelayedHeader() {
        return delayedHeader;
    }

    public boolean closeExecutorOnChannelClose() {
        return closeExecutorOnChannelClose;
    }
}
