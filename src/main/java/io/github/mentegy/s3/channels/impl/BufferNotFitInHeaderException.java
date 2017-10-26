package io.github.mentegy.s3.channels.impl;

public class BufferNotFitInHeaderException extends IllegalStateException {
    public BufferNotFitInHeaderException(int bufferSize, int position, int headerSize) {
        super("Input buffer (" + bufferSize + " bytes) " +
                "could not be located in header (" + headerSize + " bytes) " +
                "starting from position " + position);
    }
}
