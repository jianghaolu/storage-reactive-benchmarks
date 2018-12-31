import com.google.common.collect.Maps;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;
import reactor.netty.http.client.HttpClient.ResponseReceiver;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

/**
 * A client that support uploading and downloading files to and from Azure Storage Blobs.
 */
public class ReactorNettyFileClient implements FileClientReactor {
    private final String baseUrl;
    private final String host;
    private final int port;
    private String sasToken;

    public ReactorNettyFileClient(String baseUrl) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl : baseUrl + "/";
        if (baseUrl.startsWith("https")) {
            port = 443;
        } else {
            port = 80;
        }
        URI uri = null;
        try {
            uri = new URI(baseUrl);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Base URL" + baseUrl + " is not a valid URI.");
        }
        this.host = uri.getHost();
    }

    public ReactorNettyFileClient(String accountName, String container, String scheme) {
        this(String.format("%s://%s.blob.core.windows.net/%s/", scheme, accountName, container));
    }

    public ReactorNettyFileClient(String accountName, String container) {
        this(accountName, container, "https");
    }

    public String baseUrl() {
        return baseUrl;
    }

    public String host() {
        return host;
    }

    public int port() {
        return port;
    }

    public ReactorNettyFileClient withSasToken(String sasToken) {
        this.sasToken = sasToken;
        return this;
    }

    public String sasToken() {
        return sasToken;
    }

    @Override
    public Flux<ByteBuf> streamAsync(String blobName, long fileSize, long blockSize) {
        return null;
//        return Observable.range(0, (int) Math.ceil((double) fileSize / blockSize))
//                .map(i -> i * blockSize)
//                .concatMap(pos -> {
//                    long end = pos + blockSize - 1;
//                    if (end >= fileSize) {
//                        end = fileSize - 1;
//                    }
//                    return Observable.just(String.format("bytes=%d-%d", pos, end))
//                            .map(header -> {
//                                DefaultHttpRequestProvider request = new DefaultHttpRequestProvider(HttpVersion.HTTP_1_1, HttpMethod.GET, String.format("%s%s", baseUrl() + blobName, sasToken));
//                                request.setHeader("x-ms-range", header);
//                                return request;
//                            })
//                            .concatMap(req -> client.sendRequestAsync(req));
//                });
    }

    public Flux<Long> downloadAsync(String blobName, String filePath, long fileSize, long blockSize) {
        File file = new File(filePath);
        FileChannel fileChannel;
        try {
            file.createNewFile();
            fileChannel = new RandomAccessFile(file, "rw").getChannel();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        final HttpClient reactorClient =
                HttpClient.create()
//                        .tcpConfiguration(tcpClient -> tcpClient.proxy(po -> po.type(Proxy.HTTP).address(new InetSocketAddress("localhost", 8888))))
                        .port(port)
                        .baseUrl(host);



        return Flux.range(0, (int) Math.ceil((double) fileSize / blockSize))
                .map(i -> i * blockSize)
                .flatMap(pos -> {
                    long end = pos + blockSize - 1;
                    if (end >= fileSize) {
                        end = fileSize - 1;
                    }
                    return Mono.just(String.format("bytes=%d-%d", pos, end))
                            .map(header -> {
                                ResponseReceiver<?> req =  reactorClient
                                        .headers(headers -> {
                                            headers.set("Content-Length", "0")
                                                   .set("x-ms-version", "2017-04-17")
                                                   .set("x-ms-date", ZonedDateTime.now(ZoneId.of("UTC")).format(DateTimeFormatter.RFC_1123_DATE_TIME))
                                                   .set("x-ms-range", header);

                                        })
                                        .get()
                                        .uri(String.format("%s%s", baseUrl() + blobName, sasToken));
                                        return Maps.immutableEntry(pos, req);
                            });
                })
                .flatMap(req -> req.getValue().responseContent().aggregate()
                        .flatMap(buf -> {
                            int readable = buf.readableBytes();
//                                        System.out.println("Receiving " + readable + " bytes at position " + req.getKey());
                            try {
                                buf.readBytes(fileChannel, req.getKey(), readable);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                            return Mono.just((long) readable);
                        }))
                .doOnComplete(() -> {
                    try {
                        fileChannel.close();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    public Flux<Long> uploadPageBlobAsync(String blobName, String filePath, long fileSize, long blockSize) {
        if (fileSize % 512 != 0) {
            throw new UnsupportedOperationException("Page blob size must be a multiple of 512");
        }

        final String blobUrl = String.format("%s%s", baseUrl() + blobName, sasToken);
        final String pageUrl = String.format("%s%s&comp=page", baseUrl() + blobName, sasToken);

        RandomAccessFile file;
        try {
            file = new RandomAccessFile(filePath, "r");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        final FileChannel fileChannel = file.getChannel();

        final NioEventLoopGroup eventLoopGroup = new NioEventLoopGroup();
        final HttpClient reactorClient =
                HttpClient.create()
                        .tcpConfiguration(tcpClient -> tcpClient.bootstrap(bootstrap -> {
                            bootstrap.group(eventLoopGroup);
                            bootstrap.channel(NioSocketChannel.class);
                            bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
                            bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) TimeUnit.MINUTES.toMillis(3L));
                            return bootstrap;
                        }))
//                        .tcpConfiguration(tcpClient -> tcpClient.proxy(po -> po.type(Proxy.HTTP).address(new InetSocketAddress("localhost", 8888))))
                        .chunkedTransfer(false)
//                        .secure()
                        .port(port)
                        .baseUrl(host);

        return reactorClient.put().uri(blobUrl).send((r, out) -> {
            r.addHeader("Content-Length", "0");
            r.addHeader("x-ms-version", "2017-04-17");
            r.addHeader("x-ms-blob-type", "PageBlob");
            r.addHeader("x-ms-blob-content-length", String.valueOf(fileSize));
            r.addHeader("x-ms-date", ZonedDateTime.now(ZoneId.of("UTC")).format(DateTimeFormatter.RFC_1123_DATE_TIME));
            return out;
        }).response().flux()
        .flatMap(res -> Flux.range(0, (int) Math.ceil((double) fileSize / blockSize)))
        .map(i -> i* blockSize)
        .flatMap(pos -> {
            long count = pos + blockSize > fileSize ? (fileSize - pos) : blockSize;
            FileByteBufSource source = new FileByteBufSource(fileChannel, pos, count, count, ByteBufAllocator.DEFAULT);
            ResponseReceiver<?> req = reactorClient.put().uri(pageUrl).send((r, out) -> {
                r.addHeader("content-length", String.valueOf(count));
                r.addHeader("x-ms-range", String.format("bytes=%d-%d", pos, pos + count - 1));
                r.addHeader("x-ms-version", "2017-04-17");
                r.addHeader("x-ms-page-write", "Update");
                r.addHeader("x-ms-date", ZonedDateTime.now(ZoneId.of("UTC")).format(DateTimeFormatter.RFC_1123_DATE_TIME));
                return out.send(Mono.fromCallable(() -> source.read()));//.sendFile(Paths.get(filePath), pos, count);
            });
            return req.response().map(r -> count);
        }, eventLoopGroup.executorCount())
//        .flatMap(req -> req.getValue().response().map(r -> req.getKey()), concurrency)
        .doOnComplete(() -> {
            try {
                fileChannel.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });


//        DefaultHttpRequestProvider create = new DefaultHttpRequestProvider(HttpVersion.HTTP_1_1,
//                HttpMethod.PUT,
//                blobUrl);
//        create.setHeader("Content-Length", "0");
//        create.setHeader("x-ms-version", "2017-04-17");
//        create.setHeader("x-ms-blob-type", "PageBlob");
//        create.setHeader("x-ms-blob-content-length", String.valueOf(fileSize));
//        create.setHeader("x-ms-date", ZonedDateTime.now(ZoneId.of("UTC")).format(DateTimeFormatter.RFC_1123_DATE_TIME));

//        return null;
//        return client.sendRequestAsync(create)
//                .flatMap(res -> Observable.range(0, (int) Math.ceil((double) fileSize / blockSize))
//                        .map(i -> i * blockSize)
//                        .map(pos -> {
//                            long count = pos + blockSize > fileSize ? (fileSize - pos) : blockSize;
//                            FileByteBufSource source = new FileByteBufSource(fileChannel, pos, count, count, ByteBufAllocator.DEFAULT);
//                            DefaultHttpRequestProvider request = new DefaultHttpRequestProvider(HttpVersion.HTTP_1_1,
//                                    HttpMethod.PUT,
//                                    pageUrl,
//                                    source);
//                            request.setHeader("x-ms-range", String.format("bytes=%d-%d", pos, pos + count - 1));
//                            request.setHeader("Content-Length", String.valueOf(count));
//                            request.setHeader("x-ms-version", "2017-04-17");
//                            request.setHeader("x-ms-page-write", "Update");
//                            request.setHeader("x-ms-date", ZonedDateTime.now(ZoneId.of("UTC")).format(DateTimeFormatter.RFC_1123_DATE_TIME));
//                            return Maps.immutableEntry(count, request);
//                        })
//                        .flatMap(req -> {
////                            System.out.println("Sending " + req.getKey() + " bytes at position " + req.getValue().header("x-ms-range"));
//                            return client.sendRequestAsync(req.getValue()).map(r -> req.getKey());
//                        }, concurrency));
    }

    @Override
    public Flux<Long> uploadBlockBlobAsync(String blobName, String filePath, long fileSize, long blockSize) {
        BlockIdGenerator generator = new BlockIdGenerator(fileSize, blockSize);
        final String blockUrl = String.format("%s%s&comp=block", baseUrl() + blobName, sasToken);
        final String blockListUrl = String.format("%s%s&comp=blocklist", baseUrl() + blobName, sasToken);

        RandomAccessFile file;
        try {
            file = new RandomAccessFile(filePath, "r");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        final FileChannel fileChannel = file.getChannel();

        return null;
//        return Observable.range(0, (int) Math.ceil((double) fileSize / blockSize))
//                .map(i -> i * blockSize)
//                .flatMap(pos -> {
//                    long end = pos + blockSize - 1;
//                    if (end >= fileSize) {
//                        end = fileSize - 1;
//                    }
//                    return Observable.just(String.format("bytes=%d-%d", pos, end))
//                            .map(header -> {
//                                long count = pos + blockSize > fileSize ? (fileSize - pos) : blockSize;
//                                FileByteBufSource source = new FileByteBufSource(fileChannel, pos, count, count, ByteBufAllocator.DEFAULT);
//                                DefaultHttpRequestProvider request = new DefaultHttpRequestProvider(HttpVersion.HTTP_1_1,
//                                        HttpMethod.PUT,
//                                        String.format("%s&blockId=%s", blockUrl, generator.getBlockId()),
//                                        source);
//                                request.setHeader("Content-Length", String.valueOf(count));
//                                request.setHeader("x-ms-version", "2017-04-17");
//                                request.setHeader("x-ms-date", ZonedDateTime.now(ZoneId.of("UTC")).format(DateTimeFormatter.RFC_1123_DATE_TIME));
//                                return Maps.immutableEntry(count, request);
//                            });})
//                            .flatMap(req -> {
//                                System.out.println("Sending " + req.getKey() + " bytes at position " + req.getValue().header("x-ms-range"));
//                                return client.sendRequestAsync(req.getValue()).map(r -> req.getKey());
//                            }, concurrency)
//                            .doOnCompleted(() -> {
//                                try {
//                                    fileChannel.close();
//                                } catch (IOException e) {
//                                    throw new RuntimeException(e);
//                                }
//                            })
//                            .concatWith(Observable.defer(() -> {
//                                DefaultHttpRequestProvider request = new DefaultHttpRequestProvider(HttpVersion.HTTP_1_1,
//                                        HttpMethod.PUT,
//                                        blockListUrl,
//                                        new ByteArrayByteBufSource(generator.getBlockListXml().getBytes()));
//                                request.setHeader("x-ms-date", ZonedDateTime.now(ZoneId.of("UTC")).format(DateTimeFormatter.RFC_1123_DATE_TIME));
//                                return client.sendRequestAsync(request).map(b -> 0L);
//                            }));
    }
}
