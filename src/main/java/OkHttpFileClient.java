import com.google.common.io.ByteStreams;
import com.google.common.reflect.TypeToken;
import com.microsoft.azure.CloudException;
import com.microsoft.rest.DateTimeRfc1123;
import com.microsoft.rest.LogLevel;
import com.microsoft.rest.ServiceResponse;
import com.microsoft.rest.ServiceResponseBuilder;
import com.microsoft.rest.interceptors.LoggingInterceptor;
import com.microsoft.rest.protocol.ResponseBuilder;
import com.microsoft.rest.protocol.SerializerAdapter;
import com.microsoft.rest.serializer.JacksonAdapter;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import okhttp3.Interceptor;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.ResponseBody;
import org.joda.time.DateTime;
import retrofit2.Call;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.adapter.rxjava.RxJavaCallAdapterFactory;
import retrofit2.converter.scalars.ScalarsConverterFactory;
import retrofit2.http.Body;
import retrofit2.http.GET;
import retrofit2.http.Header;
import retrofit2.http.Headers;
import retrofit2.http.PUT;
import retrofit2.http.Path;
import retrofit2.http.Query;
import retrofit2.http.Streaming;
import rx.Observable;
import rx.functions.Func1;
import rx.schedulers.Schedulers;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.Proxy.Type;
import java.nio.channels.FileChannel;
import java.util.concurrent.TimeUnit;

public class OkHttpFileClient implements FileClient {
    String sasToken;
    private Retrofit retrofit;
    private FileService service;
    private StorageService storageService;
    private ResponseBuilder.Factory responseBuilderFactory;
    private SerializerAdapter<?> serializerAdapter;

    public OkHttpFileClient(String baseUrl, String sasToken) {
        OkHttpClient.Builder httpBuilder = new OkHttpClient.Builder()
                .readTimeout(5, TimeUnit.MINUTES)
                .connectTimeout(1, TimeUnit.MINUTES);
//                .proxy(new Proxy(Type.HTTP, new InetSocketAddress("localhost", 8888)));
        httpBuilder = httpBuilder.addInterceptor(new SasTokenInterceptor(sasToken));
//        httpBuilder.addInterceptor(new LoggingInterceptor(LogLevel.BASIC));
        Retrofit retrofit = new Retrofit.Builder()
                .client(httpBuilder.build())
                .addCallAdapterFactory(RxJavaCallAdapterFactory.create())
                .addConverterFactory(ScalarsConverterFactory.create())
                .baseUrl(baseUrl)
                .build();

        this.retrofit = retrofit;
        this.responseBuilderFactory = new ServiceResponseBuilder.Factory();
        this.serializerAdapter = new JacksonAdapter();
        service = retrofit.create(FileService.class);
        storageService = retrofit.create(StorageService.class);
    }

    @Override
    public String baseUrl() {
        return retrofit.baseUrl().toString();
    }

    public String sasToken() {
        return sasToken;
    }

    @Override
    public Observable<ByteBuf> streamAsync(String blobName, long fileSize, long blockSize) {
        return null;
    }

    @Override
    public Observable<Long> downloadAsync(String blobName, String filePath, long fileSize, long blockSize) {
        File file = new File(filePath);
        FileChannel fileChannel;
        try {
            file.createNewFile();
            fileChannel = new RandomAccessFile(file, "rw").getChannel();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return Observable.range(0, (int) Math.ceil((double) fileSize / blockSize))
                .map(i -> i * blockSize)
                .flatMap(pos -> {
                    long end = pos + blockSize - 1;
                    if (end >= fileSize) {
                        end = fileSize - 1;
                    }
                    return Observable.just(String.format("bytes=%d-%d", pos, end))
                            .flatMap(range -> getBlobAsync(blobName, range))
                            .subscribeOn(Schedulers.computation())
                            .map(b -> {
                                ByteBuf buf = Unpooled.wrappedBuffer(b);
                                int readable = buf.readableBytes();
//                                        System.out.println("Receiving " + readable + " bytes at position " + req.getKey());
                                try {
                                    buf.readBytes(fileChannel, pos, readable);
                                    buf.release();
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                                return (long) readable;
                            });
                });
    }

    @Override
    public Observable<Long> uploadPageBlobAsync(String blobName, String filePath, long fileSize, long blockSize) {
        return null;
    }

    @Override
    public Observable<Long> uploadBlockBlobAsync(String blobName, String filePath, long fileSize, long blockSize) {
        return null;
    }

    public interface StorageService {
        @Headers({ "Content-Type: application/json; charset=utf-8", "x-ms-logging-context: fixtures.bodyfile.FileClient getFile" })
        @GET("{blob}")
        Observable<Response<ResponseBody>> getBlob(@Path("blob") String blob, @Header("x-ms-range") String range, @Header("x-ms-date") String date, @Header("x-ms-version") String version);
    }

    public Observable<byte[]> getBlobAsync(String blob, String range) {
        return storageService.getBlob(blob, range, new DateTimeRfc1123(DateTime.now()).toString(), "2017-04-17")
                .map(r -> {
                    try {
                        InputStream stream = getBlobDelegate(r).body();
                        return ByteStreams.toByteArray(stream);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    private ServiceResponse<InputStream> getBlobDelegate(Response<ResponseBody> response) throws CloudException, IOException {
        return this.responseBuilderFactory.<InputStream, CloudException>newInstance(this.serializerAdapter)
                .register(200, new TypeToken<InputStream>() { }.getType())
                .register(206, new TypeToken<InputStream>() { }.getType())
                .registerError(CloudException.class)
                .build(response);
    }

    public interface FileService {
        @Headers({ "Content-Type: application/json; charset=utf-8", "x-ms-logging-context: fixtures.bodyfile.FileClient getFile" })
        @GET("downloads/{size}.dat")
        @Streaming
        Observable<Response<ResponseBody>> getFileAsync(@Path("size") String size);

        @Headers({ "Content-Type: application/json; charset=utf-8", "x-ms-logging-context: fixtures.bodyfile.FileClient getFile" })
        @GET("downloads/{size}.dat")
        @Streaming
        Call<ResponseBody> getFile(@Path("size") String size);

        @Headers({ "Content-Type: application/octet-stream", "x-ms-version: 2017-04-17" })
        @PUT("uploads/{blobName}?comp=block")
        @Streaming
        Observable<Response<ResponseBody>> uploadBlob(@Path("blobName") String blobName, @Query("blockid") String blockId, @Body RequestBody fileContent);

        @Headers({ "Content-Type: application/xml", "x-ms-version: 2017-04-17" })
        @PUT("uploads/{blobName}?comp=blocklist")
        Observable<Response<ResponseBody>> commitBlob(@Path("blobName") String blobName, @Body String blockListXml);
    }

    public InputStream getFile(String size) {
        return getFileWithServiceResponse(size).body();
    }

    public ServiceResponse<InputStream> getFileWithServiceResponse(String size) {
        try {
            return getFileDelegate(service.getFile(size).execute());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Observable<InputStream> getFileAsync(String size) {
        return getFileWithServiceResponseAsync(size).map(new Func1<ServiceResponse<InputStream>, InputStream>() {
            @Override
            public InputStream call(ServiceResponse<InputStream> response) {
                return response.body();
            }
        });
    }

    public Observable<ServiceResponse<InputStream>> getFileWithServiceResponseAsync(String size) {
        return service.getFileAsync(size)
                .flatMap(new Func1<Response<ResponseBody>, Observable<ServiceResponse<InputStream>>>() {
                    @Override
                    public Observable<ServiceResponse<InputStream>> call(Response<ResponseBody> response) {
                        try {
                            ServiceResponse<InputStream> clientResponse = getFileDelegate(response);
                            return Observable.just(clientResponse);
                        } catch (Throwable t) {
                            return Observable.error(t);
                        }
                    }
                });
    }

    private ServiceResponse<InputStream> getFileDelegate(Response<ResponseBody> response) throws CloudException, IOException {
        return this.responseBuilderFactory.<InputStream, CloudException>newInstance(this.serializerAdapter)
                .register(200, new TypeToken<InputStream>() { }.getType())
                .registerError(CloudException.class)
                .build(response);
    }

    public Observable<Void> uploadAsync(String blobName, String blockId, byte[] bytes) {
        return uploadWithServiceResponseAsync(blobName, blockId, bytes).map(new Func1<ServiceResponse<Void>, Void>() {
            @Override
            public Void call(ServiceResponse<Void> response) {
                return response.body();
            }
        });
    }

    public Observable<ServiceResponse<Void>> uploadWithServiceResponseAsync(String blobName, String blockId, byte[] bytes) {
        return service.uploadBlob(blobName, blockId, RequestBody.create(MediaType.parse("application/octet-stream"), bytes))
                .flatMap(new Func1<Response<ResponseBody>, Observable<ServiceResponse<Void>>>() {
                    @Override
                    public Observable<ServiceResponse<Void>> call(Response<ResponseBody> response) {
                        try {
                            ServiceResponse<Void> clientResponse = uploadDelegate(response);
                            return Observable.just(clientResponse);
                        } catch (Throwable t) {
                            return Observable.error(t);
                        }
                    }
                });
    }

    private ServiceResponse<Void> uploadDelegate(Response<ResponseBody> response) throws CloudException, IOException {
        return this.responseBuilderFactory.<Void, CloudException>newInstance(this.serializerAdapter)
                .register(201, new TypeToken<Void>() { }.getType())
                .registerError(CloudException.class)
                .build(response);
    }

    public Observable<Void> commitAsync(String blobName, String blockListXml) {
        return commitWithServiceResponseAsync(blobName, blockListXml).map(new Func1<ServiceResponse<Void>, Void>() {
            @Override
            public Void call(ServiceResponse<Void> response) {
                return response.body();
            }
        });
    }

    public Observable<ServiceResponse<Void>> commitWithServiceResponseAsync(String blobName, String blockListXml) {
        return service.commitBlob(blobName, blockListXml)
                .flatMap(new Func1<Response<ResponseBody>, Observable<ServiceResponse<Void>>>() {
                    @Override
                    public Observable<ServiceResponse<Void>> call(Response<ResponseBody> response) {
                        try {
                            ServiceResponse<Void> clientResponse = commitDelegate(response);
                            return Observable.just(clientResponse);
                        } catch (Throwable t) {
                            return Observable.error(t);
                        }
                    }
                });
    }

    private ServiceResponse<Void> commitDelegate(Response<ResponseBody> response) throws CloudException, IOException {
        return this.responseBuilderFactory.<Void, CloudException>newInstance(this.serializerAdapter)
                .register(201, new TypeToken<Void>() { }.getType())
                .registerError(CloudException.class)
                .build(response);
    }

    private static class SasTokenInterceptor implements Interceptor {
        private String sasToken;

        public SasTokenInterceptor(String sasToken) {
            this.sasToken = sasToken;
        }

        @Override
        public okhttp3.Response intercept(Chain chain) throws IOException {
            Request request = chain.request();
            String url = request.url().toString();
            Request.Builder newBuilder = request.newBuilder();
            if (url.contains("?")) {
                newBuilder.url(url.concat(sasToken.replace("?", "&")));
            } else {
                newBuilder.url(url.concat(sasToken));
            }
            return chain.proceed(newBuilder.build());
        }
    }

}