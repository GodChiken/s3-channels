package io.github.mentegy.s3.channels.util;

import java.io.IOException;
import java.nio.channels.ByteChannel;

public interface CancelableByteChannel extends ByteChannel {
    void cancel() throws IOException;
}
