package io.github.mentegy.s3.channels.testutils;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Objects;
import java.util.Random;

public class FileGenerator {

    public static TempFile randomTempFile(int size) {
        try {
            Path p = Files.createTempFile("s3", "channels");
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] content = new byte[size];
            Random rand = new Random();
            rand.nextBytes(content);
            md.update(content);
            Files.write(p, content, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
            byte[] sha1 = md.digest();
            p.toFile().deleteOnExit();
            return new TempFile(p, size, sha1);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    public static class TempFile {
        public final Path path;
        public final long size;
        public final byte[] md5;

        public TempFile(Path path, long size, byte[] md5) {
            this.path = path;
            this.size = size;
            this.md5 = md5;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TempFile tempFile = (TempFile) o;
            return size == tempFile.size &&
                    Objects.equals(path, tempFile.path) &&
                    Arrays.equals(md5, tempFile.md5);
        }

        @Override
        public int hashCode() {
            return Objects.hash(path, size, md5);
        }
    }
}
