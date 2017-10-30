# S3 Channels
[![Build Status](https://travis-ci.org/mentegy/s3-channels.svg?branch=master)](https://travis-ci.org/mentegy/s3-channels)
[![codecov](https://codecov.io/gh/mentegy/s3-channels/branch/master/graph/badge.svg)](https://codecov.io/gh/mentegy/s3-channels)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.github.mentegy/s3-channels/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.github.mentegy/s3-channels)
[![Javadocs](https://www.javadoc.io/badge/io.github.mentegy/s3-channels.svg)](https://www.javadoc.io/doc/io.github.mentegy/s3-channels)

Are you working closely with Java NIO, channels and byte buffers? Does your app requires also usage of `s3`. Tired of mapping the `java.nio.*` with `java.io.*` (which comes from AWS Java SDK). Regular `java.io.File` download/upload is not enough for you? Looking for `java.nio.channels.*` implementations for S3? Then this library is the perfect fit for you!

# Introduction
Library itself is very tiny and adds only single dependency of [AWS Java SDK](https://github.com/aws/aws-sdk-java).
Before getting started, users should keep in mind that S3 has several limitations which limits full usage of `java.nio.channels.*`. This leads to the fact that we can only read data from S3 or upload, editing is prohibited.

# Getting Started
Firstly, let's add dependencies to our faivorite build tool, for example:
### Maven dependency
```xml
<dependency>
    <groupId>io.github.mentegy</groupId>
    <artifactId>s3-channels</artifactId>
    <version>0.2.0</version>
</dependency>
```
### sbt dependency
```scala
libraryDependencies += "io.github.mentegy" % "s3-channels" % "0.2.0"
```

### Examples

Now, once we have our dependecy in a classpath, we can proceed with coding. S3 channels requires `AmazonS3` client:
```java
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

String bucket = "test-bucket";
String key = "test-object-bucket";
AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
```
Once we set this up, using S3 readable object channel as easy as:
```java
import io.github.mentegy.s3.channels.S3ReadableObjectChannel;
import java.nio.ByteBuffer;

ByteBuffer buffer = ByteBuffer.allocate(1024);
S3ReadableObjectChannel channel = S3ReadableObjectChannel
        .builder()
        .amazonS3(s3)
        .bucket(bucket)
        .key(key)
        .build();

// relative position read
channel.position(2048);
channel.read(buffer);
// or absolute position read
channel.read(buffer, 2048);
channel.close();
```

Or we can use S3 writable object channel (requires S3 multi-part upload id):
```java
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import io.github.mentegy.s3.channels.S3WritableObjectChannel;
import java.nio.ByteBuffer;

ByteBuffer buffer = ByteBuffer.allocate(1024);
String uploadId = s3
        .initiateMultipartUpload(new InitiateMultipartUploadRequest(testBucket, key))
        .getUploadId();
S3WritableObjectChannel channel S3WritableObjectChannel.builder()
        .amazonS3(s3)
        .defaultExecutorService()
        .bucket(bucket)
        .key(key)
        .uploadId(uploadId);
        
channel.write(buffer);
// ...
// your writes
// ...
channel.close();
```
