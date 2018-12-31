import com.microsoft.azure.storage.StorageCredentialsSharedAccessSignature;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.rest.ServiceResponseBuilder.Factory;
import com.microsoft.rest.serializer.JacksonAdapter;
import okhttp3.HttpUrl;
import okhttp3.Interceptor;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import org.junit.BeforeClass;
import org.junit.Test;
import retrofit2.Retrofit;
import retrofit2.adapter.rxjava.RxJavaCallAdapterFactory;
import retrofit2.converter.scalars.ScalarsConverterFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

/**
 * Created by jianghlu on 7/20/17.
 */
public class OkHttpTests {

    private static OkHttpFileClient client;
    private static Benchmark benchmark;

    @BeforeClass
    public static void setup() throws Exception {
        benchmark = new Benchmark() {
            @Override
            public FileClient client(String url, String sasToken) {
                return new OkHttpFileClient(url, sasToken);
            }
        };
//        benchmark = new Benchmark() {
//            @Override
//            public Observable<Long> downloadAsync(String url, String size, int times) {
//                return Observable.range(1, times).flatMap(i ->
//                        client.getFileAsync(size).flatMap(StringObservable::from).map(b -> (long) b.length).subscribeOn(Schedulers.io()), 12);
//            }
//
//            @Override
//            public Observable<Long> uploadAsync(String url, String blobName, String filePath, long streamSize, long blockSize) {
//                BlockIdGenerator generator = new BlockIdGenerator(streamSize, blockSize);
//                try {
//                    return StringObservable.from(new FileInputStream(filePath), (int) blockSize)
//                            .flatMap(b -> client.uploadWithServiceResponseAsync(blobName, generator.getBlockId(), b)
//                                    .subscribeOn(Schedulers.io())
//                                    .map(v -> {
//                                        v.response().body().close();
//                                        return (long) b.length;
//                                    }), 8)
//                            .concatWith(Observable.defer(() -> client.commitAsync(blobName, generator.getBlockListXml())).map(v -> 0L));
//                } catch (FileNotFoundException e) {
//                    throw new RuntimeException(e);
//                }
//            }
//        };
    }

    @Test
    public void run() throws Exception {
        System.out.println("--------OKHTTP--------");
        for (int i = 0 ; i < 1; i++) {
            benchmark.run();
        }
    }
}
