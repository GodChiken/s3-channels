package io.github.mentegy.s3.channels.impl;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import io.github.mentegy.s3.channels.S3MultiPartUploadFileChannel;
import io.github.mentegy.s3.channels.util.ByteBufferUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;

public class S3MPUDelayedHeaderFileChannel extends S3MultiPartUploadFileChannel {

    protected int pos;
    protected int id;
    protected ByteBuffer partBody;
    protected ByteBuffer header;
    protected final ConcurrentLinkedQueue<UploadPartResult> done = new ConcurrentLinkedQueue<>();
    protected final ConcurrentHashMap<Integer, CompletableFuture<Void>> workers = new ConcurrentHashMap<>();
    protected volatile boolean cancellation = false;
    protected volatile Throwable error = null;

    public S3MPUDelayedHeaderFileChannel(String key, String bucket, String uploadId, int partSize, AmazonS3 s3,
                                         ExecutorService executor, boolean cancelOnFailureInDedicatedThread) {
        super(key, bucket, uploadId, partSize, s3, executor, cancelOnFailureInDedicatedThread);
        this.header = ByteBuffer.allocate(this.partSize);
        this.partBody = ByteBuffer.allocate(this.partSize);
        this.id = 2;
        this.pos = 0;
    }

    /**
     * Writes data by given position without changing current position.
     *
     * Current implementation allows only to write data by position only for first 5MB (s3 part size) bytes.
     *
     * @throws IllegalStateException if input data and given position together overflows header buffer range
     */
    @Override
    public int write(ByteBuffer src, long position) {
        checkOnError();
        int bytes = src.remaining();
        if (position + bytes > MIN_PART_SIZE) {
            throw new IllegalStateException("Input data does not fit within header");
        } else {
            header.position((int)position);
            header.put(src);
            return bytes;
        }
    }

    /**
     * Writes a sequence of bytes to this channel from the given buffer. Bytes are written starting at this channel's
     * current position.
     * <p>
     * This call is non-blocking, actual upload of pending bytes (once reached partSize limit) will be uploaded within
     * dedicated thread.
     */
    @Override
    public int write(ByteBuffer src) {
        checkOnError();
        int bytes = src.remaining();

        if (pos < header.capacity()) {
            header.position(pos);
            if (header.remaining() <= bytes) {
                header.put(src);
                pos += bytes;
                return bytes;
            } else {
                bytes = ByteBufferUtils.putBiggerBuffer(header, src);
                pos += bytes;
                return bytes + write(src);
            }
        } else if (partBody.remaining() >= bytes) {
            partBody.put(src);
            pos += bytes;
            return bytes;
        } else {
            bytes = ByteBufferUtils.putBiggerBuffer(partBody, src);
            uploadCurrent();
            pos += bytes;
            return bytes + write(src);
        }
    }

    /**
     * Completes multipart upload.
     * <p>
     * Blocking call. Initiates uploading of last part (exceptional part which could contain less bytes han allowed),
     * waits until all workers are finished (all pending part uploads finished) and sends complete requests.
     * <p>
     * If any error occurred during uploading of last parts or complete requests, the aborting of multipart upload
     * requests will be send to s3.
     * <p>
     * If {@link S3MPUDelayedHeaderFileChannel#cancelOnFailureInDedicatedThread} is <code>true</code> then cancellation
     * will be performing within another thread hence user will be proceeding with occurred error immediately
     * without waiting on cancellation.
     */
    @Override
    @SuppressWarnings("unchecked")
    protected void implCloseChannel() throws IOException {
        uploadHeader();
        uploadLastPart();
        CompletableFuture<Void>[] futures;
        synchronized (workers) {
            futures = workers.values().toArray(new CompletableFuture[workers.size()]);
        }
        CompletableFuture<Void> complete = CompletableFuture
                .allOf(futures)
                .thenApply((x) -> {
                    s3.completeMultipartUpload(new CompleteMultipartUploadRequest()
                            .withBucketName(bucket)
                            .withKey(key)
                            .withUploadId(uploadId)
                            .withPartETags(done));
                    return null;
                });

        CompletableFuture<Void> handler = complete.handleAsync((res, err) -> {
            if (err != null) {
                cancel();
            }
            return null;
        }, executor);

        if (!cancelOnFailureInDedicatedThread) {
            complete = handler;
        }

        try {
            complete.get();
        } catch (Exception e) {
            throw new IllegalStateException("Exception during complete of multipart upload", e);
        }
    }

    /**
     * Aborts multipart upload
     */
    public void cancel() {
        cancellation = true;
        s3.abortMultipartUpload(new AbortMultipartUploadRequest(bucket, key, uploadId));
    }

    protected void uploadHeader() {
        startWorker(createRequest(1, header), 0);
    }

    protected void uploadLastPart() {
        partBody.flip();
        startWorker(createRequest(id, partBody).withLastPart(true), 0);
    }

    protected UploadPartRequest createRequest(int id, ByteBuffer buffer) {
        buffer.rewind();
        return new UploadPartRequest()
                .withBucketName(bucket)
                .withKey(key)
                .withUploadId(uploadId)
                .withPartNumber(id)
                .withPartSize(buffer.limit())
                .withInputStream(new ByteArrayInputStream(buffer.array(), 0, buffer.limit()));
    }

    protected void uploadCurrent() {
        if (id > MAX_PARTS) {
            throw new IllegalStateException("Cannot upload more parts. Reached max parts number limit (10000). Please finish current upload");
        } else {
            UploadPartRequest req = createRequest(id, partBody);
            id += 1;
            partBody = ByteBuffer.allocate(partSize);
            startWorker(req, 0);
        }
    }

    protected void startWorker(UploadPartRequest req, int retries) {
        int id = req.getPartNumber();

        CompletableFuture<Void> f = CompletableFuture
                .supplyAsync(() -> s3.uploadPart(req), executor)
                .handle((res, error) -> {
                    workers.remove(id);
                    if (res != null) {
                        done.add(res);
                    }
                    if (error != null && !cancellation) {
                        if (retries < 3) startWorker(req, retries + 1);
                        this.error = new IllegalStateException("Could not upload part " + id + " after "
                                + retries + " retries. Aborting upload", error);
                        cancel();
                    }
                    return null;
                });

        workers.put(id, f);
    }

    private void checkOnError() {
        if (error != null) {
            throw new IllegalStateException("Caught error during uploading part to s3", error);
        }
    }


    /**
     * Skips given amount of bytes, e.g. writes 0.
     *
     * @param bytes bytes to skip
     * @return this channel
     */
    public S3MPUDelayedHeaderFileChannel skip(int bytes) {
        write(ByteBuffer.allocate(bytes));
        return this;
    }

    /**
     * Sets new position to this channel.
     * <p>
     * Since we can only append using multipart upload, setting lower position than current will have no effect.
     *
     * @param newPosition new position
     * @return this channel
     */
    @Override
    public S3MPUDelayedHeaderFileChannel position(long newPosition) {
        if (newPosition > position()) {
            return skip((int) (newPosition - position()));
        }
        return this;
    }

    @Override
    public long position() {
        return pos;
    }

    @Override
    public long size() {
        return position();
    }
}
