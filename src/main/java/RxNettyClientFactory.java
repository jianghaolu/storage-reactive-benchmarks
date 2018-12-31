import io.netty.buffer.ByteBuf;
import io.reactivex.netty.RxNetty;
import io.reactivex.netty.pipeline.ssl.DefaultFactories;
import io.reactivex.netty.protocol.http.client.HttpClient;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;
import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.MediaType;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import okio.BufferedSource;
import okio.Okio;
import org.joda.time.DateTime;
import rx.Observable;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.functions.Func1;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by jianghlu on 7/20/17.
 */
public class RxNettyClientFactory implements Call.Factory {

    private HttpClient<ByteBuf, ByteBuf> rxClient;

    public RxNettyClientFactory() {
        rxClient = RxNetty.<ByteBuf, ByteBuf>newHttpClientBuilder("sdkbenchmark.blob.core.windows.net", 443)
                .withSslEngineFactory(DefaultFactories.trustAll())
                .build();

    }

    @Override
    public Call newCall(final Request request) {
        return new Call() {
            private boolean executed = false;

            @Override
            public Request request() {
                return request;
            }

            @Override
            public Response execute() throws IOException {
                final BlockingQueue<ByteBuf> queue = new LinkedBlockingDeque<ByteBuf>();
                final AtomicInteger done = new AtomicInteger(0);
                final AtomicInteger length = new AtomicInteger(0);
                rxClient.submit(HttpClientRequest.createGet(request.url().toString()))
                        .flatMap(new Func1<HttpClientResponse<ByteBuf>, Observable<ByteBuf>>() {
                            @Override
                            public Observable<ByteBuf> call(HttpClientResponse<ByteBuf> byteBufHttpClientResponse) {
                                return byteBufHttpClientResponse.getContent();
                            }
                        }).doOnNext(new Action1<ByteBuf>() {
                            @Override
                            public void call(ByteBuf byteBuf) {
                                System.out.println("Receiving! " + DateTime.now().getMillis());
                                length.addAndGet(byteBuf.readableBytes());
                                queue.add(byteBuf.retain());
                            }
                        }).doOnCompleted(new Action0() {
                            @Override
                            public void call() {
                                done.incrementAndGet();
                            }
                        }).subscribe();

                InputStream stream = new InputStream() {
                    ByteBuf current;
                    @Override
                    public int read() throws IOException {
                        if (current == null) {
                            current = getNextBuf(queue);
                        }
                        if (current.isReadable()) {
                            return current.readByte();
                        } else {
//                        current.release();
                            if (queue.size() > 0 || done.get() == 0) {
                                current = getNextBuf(queue);
                                return current.readByte();
                            } else {
                                System.out.println("Done receiving! " + DateTime.now().getMillis());
                                return -1;
                            }
                        }
                    }
                };

                BufferedSource src = Okio.buffer(Okio.source(stream));
                return new Response.Builder()
                        .protocol(Protocol.HTTP_1_1)
                        .request(request)
                        .code(200)
                        .body(ResponseBody.create(MediaType.parse(
                                "application/octet-stream"),
                                length.get(),
                                src)).build();
            }

            @Override
            public void enqueue(Callback responseCallback) {
                throw new NotImplementedException();
            }

            @Override
            public void cancel() {
                throw new NotImplementedException();
            }

            @Override
            public boolean isExecuted() {
                return executed;
            }

            @Override
            public boolean isCanceled() {
                return false;
            }
        };
    }



    public ByteBuf getNextBuf(BlockingQueue<ByteBuf> bufQueue) {
        boolean interrupted = false;
        try {
            for (;;) {
                try {
                    return bufQueue.take();
                } catch (InterruptedException ignore) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
