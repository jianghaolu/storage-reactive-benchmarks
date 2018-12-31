import io.netty.buffer.ByteBuf;
import io.netty.buffer.PoolArenaMetric;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.MultithreadEventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.reactivex.netty.RxNetty;
import io.reactivex.netty.pipeline.PipelineConfigurator;
import io.reactivex.netty.pipeline.ssl.DefaultFactories;
import io.reactivex.netty.protocol.http.client.HttpClient;
import io.reactivex.netty.protocol.http.client.HttpClientPipelineConfigurator;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;
import org.junit.BeforeClass;
import org.junit.Test;
import rx.Observable;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.channels.FileChannel;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by jianghlu on 7/20/17.
 */
public class RxNettyTests {

    private static Benchmark benchmark;
    private static HttpClient<ByteBuf, ByteBuf> rxClient;

    @BeforeClass
    public static void setup() throws Exception {
        String baseUrl = Benchmark.URL.replace("https://", "").replace("http://", "");
        PipelineConfigurator<HttpClientResponse<ByteBuf>, HttpClientRequest<ByteBuf>> configurator =
                new HttpClientPipelineConfigurator<>();

        MultithreadEventLoopGroup eventLoopGroup = "Linux".equals(System.getProperty("os.name"))
                ? new EpollEventLoopGroup()
                : new NioEventLoopGroup();

        System.out.println(eventLoopGroup.executorCount());

        rxClient = RxNetty.<ByteBuf, ByteBuf>newHttpClientBuilder(baseUrl, 443)
                .withSslEngineFactory(DefaultFactories.trustAll())
                .pipelineConfigurator(configurator)
                .eventloop(eventLoopGroup)
                .channel("Linux".equals(System.getProperty("os.name")) ? EpollSocketChannel.class : NioSocketChannel.class)
                .build();

//        benchmark = new Benchmark() {
//            @Override
//            public Observable<Long> downloadAsync(String url, String size, int times) {
//                return Observable.range(1, times)
//                        .flatMap(i -> rxClient.submit(HttpClientRequest.createGet(String.format("%s/downloads/%s.dat", url, size)))
//                                .flatMap(res -> res.getContent().map(buf -> (long) buf.readableBytes())));
//            }
//
//            @Override
//            public Observable<Long> uploadAsync(String url, String blobName, String filePath, long streamSize, long blockSize) {
//                BlockIdGenerator generator = new BlockIdGenerator(streamSize, blockSize);
//                final String blockUrl = String.format("%s/disks/%s%s&comp=block", url, blobName, Benchmark.SAS_TOKEN);
//                final String blocklistUrl = String.format("%s/disks/%s%s&comp=blocklist", url, blobName, Benchmark.SAS_TOKEN);
//
//                RandomAccessFile file;
//                try {
//                    file = new RandomAccessFile(filePath, "r");
//                } catch (IOException e) {
//                    throw new RuntimeException(e);
//                }
//                final FileChannel fileChannel = file.getChannel();
//
//                final AtomicInteger cap = new AtomicInteger(50);
//
//                try {
//                    return Observable.range(0, (int) Math.ceil((float) streamSize / blockSize) - 1)
//                            .map(i -> i * blockSize)
//                            .map(pos -> {
//                                try {
//                                    long count = pos + blockSize > streamSize ? (streamSize - pos) : blockSize;
//                                    System.out.println("Thread is " + Thread.currentThread().getName());
//                                    ByteBuf direct = PooledByteBufAllocator.DEFAULT.buffer((int) count, (int) count);
//                                    direct.writeBytes(fileChannel, pos, (int) count);
//                                    return direct;
//                                } catch (IOException e) {
//                                    return null;
//                                }
//                            })
//                            .flatMap(buf -> {
//                                HttpClientRequest<ByteBuf> request = null;
//                                try {
//                                    request = HttpClientRequest.createPut(blockUrl + "&blockid=" + URLEncoder.encode(generator.getBlockId(), "UTF-8"))
//                                            .withContent(buf)
//                                            .withHeader("Content-Length", String.valueOf(buf.capacity()))
//                                            .withHeader("x-ms-date", ZonedDateTime.now(ZoneId.of("UTC")).format(DateTimeFormatter.RFC_1123_DATE_TIME));
//                                } catch (UnsupportedEncodingException e) {
//                                    e.printStackTrace();
//                                }
//                                return rxClient.submit(request).map(res -> blockSize)
////                                        .doOnSubscribe(() -> System.out.println("Ref count is " + buf.refCnt()))
//                                        .doOnCompleted(() -> {
//                                        });
//                            }).reduce((a, b) -> a + b).flatMap(sum -> {
//                                System.out.println("Committing...");
//                                HttpClientRequest<ByteBuf> request = HttpClientRequest.createPut(blocklistUrl)
//                                        .withContent(generator.getBlockListXml())
//                                        .withHeader("x-ms-date", ZonedDateTime.now(ZoneId.of("UTC")).format(DateTimeFormatter.RFC_1123_DATE_TIME));
//                                return rxClient.submit(request).map(v -> sum).doOnCompleted(() -> System.out.println("Committed"));
//                            });
//                } catch (RuntimeException e) {
//                    throw e;
//                }
//            }
//        };
    }

    private synchronized static long usedDirectoryMemory() {
        long total = 0;
        for (PoolArenaMetric metric : PooledByteBufAllocator.DEFAULT.directArenas()) {
            total += metric.numActiveBytes();
        }
        return total;
    }

    @Test
    public void run() throws Exception {
        System.out.println("--------NETTY---------");
        for (int i = 0 ; i < 1; i++) {
            benchmark.run();
        }
    }
}
