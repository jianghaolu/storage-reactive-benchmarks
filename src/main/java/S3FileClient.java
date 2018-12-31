import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import com.google.common.collect.Maps;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import rx.Emitter;
import rx.Emitter.BackpressureMode;
import rx.Observable;
import rx.functions.Action1;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.FileChannel;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A client that support uploading and downloading files to and from Azure Storage Blobs.
 */
public class S3FileClient implements FileClient {
    private final TransferManager transferManager;
    private final String bucket;

    public S3FileClient(AWSCredentialsProvider credentials, String bucket, Regions region) {
        this.transferManager = TransferManagerBuilder.standard()
                .withS3Client(AmazonS3ClientBuilder.standard().withCredentials(credentials).withRegion(region).build())
                .build();
        this.bucket = bucket;
    }

    @Override
    public String baseUrl() {
        return null;
    }

    @Override
    public Observable<ByteBuf> streamAsync(String blobName, long fileSize, long blockSize) {
        return null;
    }

    @Override
    public Observable<Long> downloadAsync(String blobName, String filePath, long fileSize, long blockSize) {
        return Observable.fromEmitter(emitter -> {
            Download download = transferManager.download(bucket, blobName, new File(filePath));
            download.addProgressListener((ProgressListener) progressEvent -> emitter.onNext(progressEvent.getBytesTransferred()));

            try {
                download.waitForCompletion();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            emitter.onCompleted();
        }, BackpressureMode.BUFFER);
    }

    @Override
    public Observable<Long> uploadPageBlobAsync(String blobName, String filePath, long fileSize, long blockSize) {
        return Observable.fromEmitter(emitter -> {
            Upload upload = transferManager.upload(bucket, blobName, new File(filePath));
            upload.addProgressListener((ProgressListener) progressEvent -> emitter.onNext(progressEvent.getBytesTransferred()));

            try {
                upload.waitForCompletion();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            emitter.onCompleted();
        }, BackpressureMode.BUFFER);
    }

    @Override
    public Observable<Long> uploadBlockBlobAsync(String blobName, String filePath, long fileSize, long blockSize) {
        return null;
    }

    void shutdown() {
        transferManager.shutdownNow();
    }
}
