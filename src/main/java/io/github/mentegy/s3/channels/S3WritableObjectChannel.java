package io.github.mentegy.s3.channels;

import com.amazonaws.services.s3.AmazonS3;
import io.github.mentegy.s3.channels.builder.S3WritableObjectChannelBuilder;
import io.github.mentegy.s3.channels.impl.S3AppendableDelayedHeaderObjectChannel;

import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

public abstract class S3WritableObjectChannel implements SeekableByteChannel, AsynchronousCancellable<Void> {

    public static final int MAX_PARTS = 10_000;
    public static final int MIN_PART_SIZE = 5 * 1024 * 1024;

    /**
     * Object key in S3
     */
    public final String key;
    /**
     * S3 bucket
     */
    public final String bucket;
    /**
     * S3 multi-part upload id
     */
    public final String uploadId;

    /**
     * Average size in bytes of each part being uploaded into S3 via multipart upload.
     * Most S3 implementations does not allow to set this values less than 5MB
     */
    public final int partSize;

    /**
     * Amazon S3 client
     */
    public final AmazonS3 s3;

    /**
     * Executor service which takes care of uploading parts into S3.
     */
    public final ExecutorService executor;

    /**
     * If {@code true} then {@link S3WritableObjectChannel#executor} will also be closed
     * on {@link S3WritableObjectChannel#close}
     */
    public final boolean closeExecutorOnClose;

    /**
     * Number of retries t re-upload failed part. If upload of failed part is still failing
     * after reaching this number then whole upload is cancelled.
     */
    public final int failedPartUploadRetries;

    protected S3WritableObjectChannel(String key, String bucket, String uploadId, int partSize, AmazonS3 s3,
                                      ExecutorService executor, boolean closeExecutorOnClose, int failedPartUploadRetries) {
        this.key = key;
        this.bucket = bucket;
        this.uploadId = uploadId;
        this.partSize = partSize;
        this.s3 = s3;
        this.executor = executor;
        this.closeExecutorOnClose = closeExecutorOnClose;
        this.failedPartUploadRetries = failedPartUploadRetries;
    }

    /**
     * Creates empty builder
     *
     * @return this channel's builder
     */
    public static S3WritableObjectChannelBuilder builder() {
        return new S3WritableObjectChannelBuilder();
    }

    /**
     * Tells whether or not this channel is {@link S3AppendableDelayedHeaderObjectChannel}
     *
     * @return {@code true} if channel is {@link S3AppendableDelayedHeaderObjectChannel}
     */
    public abstract boolean hasDelayedHeader();

    /**
     * Returns header size
     *
     * @return header size if any
     */
    public abstract int headerSize();

    /**
     * Writes a sequence of bytes to this channel from the given buffer.
     *
     * @param src The buffer from which bytes are to be retrieved
     * @return written bytes
     */
    @Override
    public abstract int write(ByteBuffer src);

    /**
     * Writes a sequence of bytes to this channel from the given buffer staring from given position.
     * <p>
     * Current position is not modified.
     *
     * @param src The buffer from which bytes are to be retrieved
     * @return written bytes
     * @throws UnsupportedOperationException if this instance is not {@link S3AppendableDelayedHeaderObjectChannel}
     */
    public abstract int write(ByteBuffer src, int position);

    /**
     * Sets this channel's position.
     * <p>
     * Setting the position to a value that is greater than the current size is legal but
     * does not change the size of the entity.
     * <p>
     * Setting the position to a value that is less than the current position is only possible
     * for {@link S3AppendableDelayedHeaderObjectChannel} with condition that given value would be less
     * than range of first part. E.g. range between {@code 0} and {@link S3WritableObjectChannel#partSize}
     *
     * @param newPosition the new position
     * @return this channel
     */
    @Override
    public abstract S3WritableObjectChannel position(long newPosition);


    /**
     * Returns this channel's position.
     *
     * @return This channel's position
     */
    @Override
    public abstract long position();

    /**
     * Returns this channel's size.
     * <p>
     * If this instance is NOT {@link S3AppendableDelayedHeaderObjectChannel} then
     * this method returns {@link S3WritableObjectChannel#position}
     *
     * @return This channel's size
     */
    @Override
    public abstract long size();


    /**
     * Completes multi-part upload.
     * <p>
     * Blocking call. Uploads all pending bytes as last part and waits until all uploading workers will be finished
     * <p>
     * If this is instance of {@link S3AppendableDelayedHeaderObjectChannel} then it also upload first part, e.g. header.
     * <p>
     * When all parts are successfully uploaded then complete multi-part upload is sent to S3.
     * This method succeeded only if every part is uploaded and complete request succeed.
     * <p>
     * In case of any failure, this method raises thrown exception and sends independently abort request to S3.
     * Abort request is non-blocking, e.g. caller of this method will not wait for abort request completion.
     * Result of abort request could be found in {@link S3WritableObjectChannel#getCancellation}
     */
    @Override
    public abstract void close();


    /**
     * Cancels current upload by sending abort request to S3.
     * <p>
     * Non-blocking call.
     *
     * @return cancellation task of issued request or already existing task if any.
     */
    public abstract CompletableFuture<Void> cancel();

    /**
     * Truncates channel to given size.
     * Since setting position is not possible for multi-part upload,
     * the region from current position to given size is written by zeros.
     * <p>
     * Changes current position
     *
     * @param size new size
     * @return this channel
     * @throws UnsupportedOperationException if new size is less than current position or greater than maz int value
     */
    @Override
    public S3WritableObjectChannel truncate(long size) {
        if (size < position() || size > Integer.MAX_VALUE) {
            throw new UnsupportedOperationException();
        }
        skip((int) (size - position()));
        return this;
    }

    /**
     * Writes given amount of bytes according to current position.
     * Since it's not possible for multi-part upload to just skip bytes,
     * byte buffer of given amount of 0 bytes is written
     * <p>
     * Changes current position
     *
     * @param size bytes to skip (write zeros)
     * @return this channel
     */
    public S3WritableObjectChannel skip(int size) {
        write(ByteBuffer.allocate(size));
        return this;
    }

    /**
     * Reads are not supported by multi-part upload channel
     */
    @Override
    public int read(ByteBuffer dst) {
        throw new UnsupportedOperationException();
    }
}
