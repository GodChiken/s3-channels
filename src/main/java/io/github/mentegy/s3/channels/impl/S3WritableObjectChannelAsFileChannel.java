package io.github.mentegy.s3.channels.impl;

import io.github.mentegy.s3.channels.AsynchronousCancellable;
import io.github.mentegy.s3.channels.S3WritableObjectChannel;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.CompletableFuture;

public class S3WritableObjectChannelAsFileChannel extends FileChannel implements AsynchronousCancellable<Void> {

    private S3WritableObjectChannel channel;

    public S3WritableObjectChannelAsFileChannel(S3WritableObjectChannel channel) {
        this.channel = channel;
    }

    /**
     * Behaves the same as {@link S3WritableObjectChannel#write(ByteBuffer, long)}
     */
    @Override
    public int write(ByteBuffer src, long position) {
        return channel.write(src, position);
    }

    /**
     * Behaves the same as {@link S3WritableObjectChannel#write(ByteBuffer)}
     */
    @Override
    public int write(ByteBuffer src) {
        return channel.write(src);
    }

    /**
     * Behaves the same as {@link S3WritableObjectChannel#write(ByteBuffer[], int, int)}
     */
    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) {
        return channel.write(srcs, offset, length);
    }

    /**
     * Behaves the same as {@link S3WritableObjectChannel#position()}
     */
    @Override
    public long position() {
        return channel.position();
    }

    /**
     * Behaves the same as {@link S3WritableObjectChannel#position(long)}
     */
    @Override
    public S3WritableObjectChannelAsFileChannel position(long newPosition) {
        channel.position(newPosition);
        return this;
    }

    /**
     * Behaves the same as {@link S3WritableObjectChannel#size()}
     */
    @Override
    public long size() {
        return channel.size();
    }

    /**
     * Behaves the same as {@link S3WritableObjectChannel#truncate(long)}
     */
    @Override
    public S3WritableObjectChannelAsFileChannel truncate(long size) {
        channel.truncate(size);
        return this;
    }

    /**
     * No effect
     *
     * @param metaData - ignored
     */
    @Override
    public void force(boolean metaData) {
        // no effect
        // maybe upload header if any?
    }

    @Override
    protected void implCloseChannel() {
        channel.close();
    }

    /**
     * Behaves the same as {@link S3WritableObjectChannel#cancel}
     */
    @Override
    public CompletableFuture<Void> cancel() {
        return channel.cancel();
    }

    /**
     * Behaves the same as {@link S3WritableObjectChannel#getCancellation()} ()}
     */
    @Override
    public CompletableFuture<Void> getCancellation() {
        return channel.getCancellation();
    }

    /**
     * Operation is not supported by this channel implementation.
     *
     * @throws UnsupportedOperationException - not supported
     */
    @Override
    public long transferFrom(ReadableByteChannel src, long position, long count) {
        throw new UnsupportedOperationException();
    }

    /**
     * Operation is not supported by this channel implementation.
     *
     * @throws UnsupportedOperationException - not supported
     */
    @Override
    public long transferTo(long position, long count, WritableByteChannel target) {
        throw new UnsupportedOperationException();
    }

    /**
     * Operation is not supported by this channel implementation.
     *
     * @throws UnsupportedOperationException - not supported
     */
    @Override
    public MappedByteBuffer map(MapMode mode, long position, long size) {
        throw new UnsupportedOperationException();
    }

    /**
     * Operation is not supported by this channel implementation.
     *
     * @throws UnsupportedOperationException - not supported
     */
    @Override
    public FileLock lock(long position, long size, boolean shared) {
        throw new UnsupportedOperationException();
    }

    /**
     * Operation is not supported by this channel implementation.
     *
     * @throws UnsupportedOperationException - not supported
     */
    @Override
    public FileLock tryLock(long position, long size, boolean shared) {
        throw new UnsupportedOperationException();
    }

    /**
     * Operation is not supported by this channel implementation.
     *
     * @throws UnsupportedOperationException - not supported
     */
    @Override
    public int read(ByteBuffer dst) {
        throw new UnsupportedOperationException();
    }

    /**
     * Operation is not supported by this channel implementation.
     *
     * @throws UnsupportedOperationException - not supported
     */
    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) {
        throw new UnsupportedOperationException();
    }


    /**
     * Operation is not supported by this channel implementation.
     *
     * @throws UnsupportedOperationException - not supported
     */
    @Override
    public int read(ByteBuffer dst, long position) {
        throw new UnsupportedOperationException();
    }
}
