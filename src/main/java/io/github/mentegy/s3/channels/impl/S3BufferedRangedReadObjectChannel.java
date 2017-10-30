package io.github.mentegy.s3.channels.impl;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import io.github.mentegy.s3.channels.util.ByteBufferUtils;

import java.io.IOException;
import java.nio.ByteBuffer;


public class S3BufferedRangedReadObjectChannel extends S3RangedReadObjectChannel {

    protected ByteBuffer buffer;
    protected long bufferOffset;
    protected long bufferLimit;

    public S3BufferedRangedReadObjectChannel(String key, String bucket, AmazonS3 s3, int bufferSize) {
        super(key, bucket, s3);
        buffer = ByteBuffer.allocate(bufferSize);
    }

    @Override
    public int read(ByteBuffer dst, long position) throws IOException {
        if (!dst.hasRemaining()) {
            return 0;
        }
        int need = dst.remaining();
        if (inRange(position)) {
            buffer.position((int)(position - bufferOffset));
            int has = buffer.remaining();
            if (need <= has) {
                return ByteBufferUtils.putBiggerBuffer(dst, buffer);
            }
            dst.put(buffer);
            return has + read(dst, position + has);
        }
        buffer.rewind();

        if (need <= buffer.capacity()) {
            S3Object object = readS3ObjectByRange(position, buffer.capacity());
            ByteBufferUtils.readFromInputStream(object.getObjectContent(), buffer, true);
            bufferOffset = position;
            bufferLimit = position + buffer.position();
            buffer.position(0);
            return ByteBufferUtils.putBiggerBuffer(dst, buffer);
        } else {
            S3Object object = readS3ObjectByRange(position, dst.remaining());
            return ByteBufferUtils.readFromInputStream(object.getObjectContent(), dst, true);
        }
    }

    @Override
    public boolean isOpen() {
        return buffer != null;
    }

    @Override
    public void close() {
        buffer = null;
        super.close();
    }


    protected S3Object readS3ObjectByRange(long start, long size) {
        return s3.getObject(new GetObjectRequest(bucket, key)
                .withRange(start, start + size));
    }

    protected boolean inRange(long position) {
        return position >= bufferOffset && position < bufferLimit;
    }
}
