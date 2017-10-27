package io.github.mentegy.s3.channels;

import com.amazonaws.services.s3.AmazonS3;
import io.github.mentegy.s3.channels.builder.S3ReadableObjectChannelBuilder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;

public abstract class S3ReadableObjectChannel implements SeekableByteChannel {

    /**
     * Object key in S3
     */
    public final String key;
    /**
     * S3 bucket
     */
    public final String bucket;
    /**
     * Amazon S3 client
     */
    public final AmazonS3 s3;

    public S3ReadableObjectChannel(String key, String bucket, AmazonS3 s3) {
        this.key = key;
        this.bucket = bucket;
        this.s3 = s3;
    }

    /**
     * Creates empty builder
     *
     * @return this channel's builder
     */
    public static S3ReadableObjectChannelBuilder builder() {
        return new S3ReadableObjectChannelBuilder();
    }

    /**
     * Reads a sequence of bytes from this channel into the given buffer starting from given position.
     * Does not change current position.
     *
     * @param dst destination buffer
     * @return read bytes
     * @throws IOException - if any
     */
    public abstract int read(ByteBuffer dst, long position) throws IOException;

    /**
     * Reads a sequence of bytes from this channel into the given buffer starting from current position.
     * Increases current position by read bytes.
     *
     * @param dst destination buffer
     * @return read bytes
     * @throws IOException - if any
     */
    @Override
    public abstract int read(ByteBuffer dst) throws IOException;

    /**
     * Returns current position
     *
     * @return current position
     */
    @Override
    public abstract long position();

    /**
     * Sets new position.
     *
     * @param newPosition new position, could not be greater than size or less than 0;
     * @return this channel
     */
    @Override
    public abstract S3ReadableObjectChannel position(long newPosition);

    /**
     * Returns this channel / s3 object size
     *
     * @return size
     */
    @Override
    public abstract long size();

    /**
     * Checks whereas this channel is open.
     * <p>
     * All current implementation are stateless
     *
     * @return for now, always {@code true}
     */
    @Override
    public abstract boolean isOpen();

    /**
     * Closes this channel.
     * <p>
     * All current implementation are stateless, this method does not effect
     */
    @Override
    public abstract void close();

    /**
     * S3 objects are readonly
     *
     * @throws UnsupportedOperationException if called
     */
    @Override
    public SeekableByteChannel truncate(long size) {
        throw new UnsupportedOperationException();
    }

    /**
     * S3 objects are readonly
     *
     * @throws UnsupportedOperationException if called
     */
    @Override
    public int write(ByteBuffer src) {
        throw new UnsupportedOperationException();
    }
}
