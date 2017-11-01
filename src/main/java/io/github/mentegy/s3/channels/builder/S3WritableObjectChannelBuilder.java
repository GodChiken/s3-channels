package io.github.mentegy.s3.channels.builder;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import io.github.mentegy.s3.channels.S3WritableObjectChannel;
import io.github.mentegy.s3.channels.impl.S3AppendableDelayedHeaderObjectChannel;
import io.github.mentegy.s3.channels.impl.S3AppendableObjectChannel;
import io.github.mentegy.s3.channels.impl.S3WritableObjectChannelAsFileChannel;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class S3WritableObjectChannelBuilder {
    private String key;
    private String bucket;
    private String uploadId;
    private int partSize = S3WritableObjectChannel.MIN_PART_SIZE;
    private int failedPartUploadRetries;
    private AmazonS3 amazonS3;
    private ExecutorService executorService;
    private boolean delayedHeader = false;
    private boolean closeExecutorOnChannelClose = false;

    /**
     * Builds instance of {@link S3WritableObjectChannel}
     * @return if {@link this#delayedHeader} is {@code true} then {@link S3AppendableDelayedHeaderObjectChannel},
     *         otherwise - {@link S3AppendableObjectChannel}
     */
    public S3WritableObjectChannel build() {
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
                new S3AppendableDelayedHeaderObjectChannel(key, bucket, uploadId, partSize, amazonS3, executorService, closeExecutorOnChannelClose, failedPartUploadRetries) :
                new S3AppendableObjectChannel(key, bucket, uploadId, partSize, amazonS3, executorService, closeExecutorOnChannelClose, failedPartUploadRetries);
    }

    /**
     * Wraps built {@link S3WritableObjectChannel} with {@link S3WritableObjectChannelAsFileChannel}
     *
     * Note, {@link S3WritableObjectChannelAsFileChannel} extends {@link java.nio.channels.FileChannel}
     * @return {@link S3WritableObjectChannelAsFileChannel}
     */
    public S3WritableObjectChannelAsFileChannel buildAsFileChannel() {
        return new S3WritableObjectChannelAsFileChannel(build());
    }

    /**
     * Retrieves bucket, key and uploadId from {@link InitiateMultipartUploadResult}
     */
    public S3WritableObjectChannelBuilder initiateMultipartUploadResult(InitiateMultipartUploadResult result) {
        bucket(result.getBucketName());
        key(result.getKey());
        uploadId(result.getUploadId());
        return this;
    }

    /**
     * S3 object key
     */
    public S3WritableObjectChannelBuilder key(String key) {
        this.key = key;
        return this;
    }

    /**
     * S3 bucket
     */
    public S3WritableObjectChannelBuilder bucket(String bucket) {
        this.bucket = bucket;
        return this;
    }

    /**
     * S3 multi-part upload id
     */
    public S3WritableObjectChannelBuilder uploadId(String uploadId) {
        this.uploadId = uploadId;
        return this;
    }

    /**
     * Average size in bytes of each part being uploaded into S3 via multipart upload.
     * Most S3 implementations does not allow to set this values less than 5MB
     *
     * Default 5MB
     */
    public S3WritableObjectChannelBuilder partSize(int partSize) {
        this.partSize = partSize;
        return this;
    }

    /**
     * Number of retries t re-upload failed part. If upload of failed part is still failing
     * after reaching this number then whole upload is cancelled.
     *
     * Default 0
     */
    public S3WritableObjectChannelBuilder failedPartUploadRetries(int failedPartUploadRetries) {
        this.failedPartUploadRetries = failedPartUploadRetries;
        return this;
    }

    /**
     * Amazon S3 client
     */
    public S3WritableObjectChannelBuilder amazonS3(AmazonS3 amazonS3) {
        this.amazonS3 = amazonS3;
        return this;
    }

    /**
     * Executor service which takes care of uploading parts into S3.
     */
    public S3WritableObjectChannelBuilder executorService(ExecutorService executorService) {
        this.executorService = executorService;
        return this;
    }

    /**
     * Sets default executor service as CachedThreadPool
     */
    public S3WritableObjectChannelBuilder defaultCachedThreadPoolExecutor() {
        closeExecutorOnChannelClose(true);
        this.executorService = Executors.newCachedThreadPool();
        return this;
    }

    /**
     * Whereas created instance should be {@link S3AppendableDelayedHeaderObjectChannel}
     *
     * Default false
     */
    public S3WritableObjectChannelBuilder delayedHeader(boolean delayedHeader) {
        this.delayedHeader = delayedHeader;
        return this;
    }

    /**
     * Whereas to close executor service on channel close
     */
    public S3WritableObjectChannelBuilder closeExecutorOnChannelClose(boolean closeExecutorOnChannelClose) {
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
