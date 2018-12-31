import io.netty.buffer.ByteBuf;
import reactor.core.publisher.Flux;
import rx.Observable;

/**
 * A client that support uploading and downloading files to and from Azure Storage Blobs.
 */
public interface FileClientReactor {
    String baseUrl();
    Flux<ByteBuf> streamAsync(String blobName, long fileSize, long blockSize);
    Flux<Long> downloadAsync(String blobName, String filePath, long fileSize, long blockSize);
    Flux<Long> uploadPageBlobAsync(String blobName, String filePath, long fileSize, long blockSize);
    Flux<Long> uploadBlockBlobAsync(String blobName, String filePath, long fileSize, long blockSize);
}
