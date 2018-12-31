import io.netty.buffer.ByteBuf;
import rx.Observable;

import java.io.IOException;

/**
 * A client that support uploading and downloading files to and from Azure Storage Blobs.
 */
public interface FileClient {
    String baseUrl();
    Observable<ByteBuf> streamAsync(String blobName, long fileSize, long blockSize);
    Observable<Long> downloadAsync(String blobName, String filePath, long fileSize, long blockSize);
    Observable<Long> uploadPageBlobAsync(String blobName, String filePath, long fileSize, long blockSize);
    Observable<Long> uploadBlockBlobAsync(String blobName, String filePath, long fileSize, long blockSize);
}
