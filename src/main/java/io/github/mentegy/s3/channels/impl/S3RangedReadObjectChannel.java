package io.github.mentegy.s3.channels.impl;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import io.github.mentegy.s3.channels.S3ReadableObjectChannel;
import io.github.mentegy.s3.channels.util.ByteBufferUtils;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A readonly, stateless (except current position) s3 object byte channel.
 * <p>
 * Very simple implementation, does not make any buffering or caching operation,
 * which makes it stateless except current position.
 * <p>
 * Maintaining current position does not make it thread-safe. However it could be
 * treated as thread-safe only bu calling read with absolute position, which does
 * not change current position {@link S3RangedReadObjectChannel#read(ByteBuffer, long)}.
 */
public class S3RangedReadObjectChannel extends S3ReadableObjectChannel {

    protected final ObjectMetadata metadata;
    protected final long size;
    protected long pos;

    public S3RangedReadObjectChannel(String key, String bucket, AmazonS3 s3) {
        super(key, bucket, s3);
        metadata = s3.getObjectMetadata(bucket, key);
        size = metadata.getContentLength();
    }

    @Override
    public int read(ByteBuffer dst, long position) throws IOException {
        S3Object object = s3.getObject(new GetObjectRequest(bucket, key)
                .withRange(position, position + dst.remaining()));
        int read = ByteBufferUtils.readFromInputStream(object.getObjectContent(), dst);
        object.close();
        return read;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        int read = read(dst, pos);
        pos += read;
        return read;
    }

    @Override
    public long position() {
        return pos;
    }

    @Override
    public S3RangedReadObjectChannel position(long newPosition) {
        if (0 <= newPosition && newPosition <= size) {
            pos = newPosition;
        }
        return this;
    }

    @Override
    public long size() {
        return size;
    }

    @Override
    public boolean isOpen() {
        return true;
    }

    @Override
    public void close() {
        // stateless channel
    }
}
