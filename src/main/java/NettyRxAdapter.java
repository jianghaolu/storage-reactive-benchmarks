import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.EmptyByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.pool.AbstractChannelPoolHandler;
import io.netty.channel.pool.ChannelPool;
import io.netty.channel.pool.FixedChannelPool;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequestEncoder;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseDecoder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.util.AttributeKey;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.Future;
import rx.Emitter;
import rx.Emitter.BackpressureMode;
import rx.Observable;

import javax.net.ssl.SSLException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;

/**
 * Created by jianghlu on 7/20/17.
 */
public class NettyRxAdapter {
    private int eventLoopGroupSize;
    private int channelPoolSize;

    private final NioEventLoopGroup eventLoopGroup;
    private final Bootstrap bootstrap;
    private SslContext sslContext;
    private final ChannelPool pool;
    private final static AttributeKey<Integer> RETRY_COUNT = AttributeKey.newInstance("retry-count");
    private final static AttributeKey<DefaultHttpRequestProvider> REQUEST_PROVIDER = AttributeKey.newInstance("request-provider");
    private final static AttributeKey<String> RANGE = AttributeKey.newInstance("range");

    public NettyRxAdapter(String host, int port) {
        this(host, port, 0, 0);
    }

    public NettyRxAdapter(String host, int port, int eventLoopGroupSize, int channelPoolSize) {
        this.eventLoopGroupSize = eventLoopGroupSize;
        this.channelPoolSize = channelPoolSize;
        bootstrap = new Bootstrap();
        if (eventLoopGroupSize != 0) {
            eventLoopGroup = new NioEventLoopGroup(eventLoopGroupSize);
        } else {
            eventLoopGroup = new NioEventLoopGroup();
            this.eventLoopGroupSize = eventLoopGroup.executorCount();
        }
        if (channelPoolSize == 0) {
            this.channelPoolSize = eventLoopGroup.executorCount() * 2;
        }

        bootstrap.group(eventLoopGroup);
        bootstrap.channel(NioSocketChannel.class);
        bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
        bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) TimeUnit.MINUTES.toMillis(3L));
        try {
            sslContext = SslContextBuilder.forClient()
                    .trustManager(InsecureTrustManagerFactory.INSTANCE).build();
        } catch (SSLException e) {
            e.printStackTrace();
        }

        pool = new FixedChannelPool(bootstrap.remoteAddress(host, port), new AbstractChannelPoolHandler() {
            @Override
            public void channelCreated(Channel ch) throws Exception {
                if (sslContext != null) {
                    ch.pipeline().addLast(sslContext.newHandler(ch.alloc(), host, port));
                }
                ch.pipeline().addLast(new HttpResponseDecoder());
                ch.pipeline().addLast(new HttpRequestEncoder());
                ch.pipeline().addLast(new RetryChannelHandler(NettyRxAdapter.this));
                ch.pipeline().addLast(new HttpClientInboundHandler(NettyRxAdapter.this));
//                ch.pipeline().addFirst(new HttpProxyHandler(new InetSocketAddress("localhost", 8888)));
            }

            @Override
            public void channelReleased(Channel ch) throws Exception {
                ch.attr(RETRY_COUNT).set(0);
            }
        }, this.channelPoolSize);
    }

    public Observable<ByteBuf> sendRequestAsync(DefaultHttpRequestProvider request) {
        return Observable.defer(() -> {
            URI uri = null;
            try {
                uri = new URI(request.uri());
                request.setHeader(HttpHeaders.Names.HOST, uri.getHost());
                request.setHeader(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
            } catch (URISyntaxException e) {
                return Observable.error(e);
            }

            Future<Channel> future = pool.acquire();

            return Observable.<ByteBuf>fromEmitter(emitter -> future.addListener(cf -> {
                if (!cf.isSuccess()) {
                    emitter.onError(cf.cause());
                    return;
                }

                Channel channel = (Channel) cf.getNow();

                channel.attr(REQUEST_PROVIDER).set(request);

                if (channel.pipeline().last() == null) {
                    channel.pipeline().addLast(new HttpResponseDecoder());
                    channel.pipeline().addLast(new HttpRequestEncoder());
                    channel.pipeline().addLast(new RetryChannelHandler(NettyRxAdapter.this));
                    channel.pipeline().addLast(new HttpClientInboundHandler(NettyRxAdapter.this));
                }

                channel.pipeline().get(HttpClientInboundHandler.class).setEmitter(emitter);


                FullHttpRequest raw = request.provide();
                String range = raw.headers().get("x-ms-range");
                channel.attr(RANGE).set(range);
                channel.writeAndFlush(raw).addListener(v -> {
                    if (v.isSuccess()) {
                        channel.read();
                    } else {
                        emitter.onError(v.cause());
                    }
                });
            }), BackpressureMode.BUFFER)
                    .retryWhen(observable -> observable.zipWith(Observable.range(1, 10), (throwable, integer) -> integer)
                            .flatMap(i -> Observable.timer(i, TimeUnit.SECONDS)))
                    .toList()
                    .map(l -> {
                        ByteBuf[] bufs = new ByteBuf[l.size()];
                        return Unpooled.wrappedBuffer(l.toArray(bufs));
                    });
        });
    }

    public NioEventLoopGroup eventLoopGroup() {
        return eventLoopGroup;
    }

    public int channelPoolSize() {
        return channelPoolSize;
    }

    private static class HttpClientInboundHandler extends ChannelInboundHandlerAdapter {

        private static final String HEADER_CONTENT_LENGTH = "Content-Length";
        private Emitter<ByteBuf> emitter;
        private NettyRxAdapter adapter;
        private long contentLength;

        public HttpClientInboundHandler(NettyRxAdapter adapter) {
            this.adapter = adapter;
        }

        public void setEmitter(Emitter<ByteBuf> emitter) {
            this.emitter = emitter;
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            adapter.pool.release(ctx.channel());
            emitter.onError(cause);
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
//            System.out.println("Reading on thread " + Thread.currentThread().getName());
            if (msg instanceof HttpResponse)
            {
                HttpResponse response = (HttpResponse) msg;
                if (response.headers().contains(HEADER_CONTENT_LENGTH)) {
                    contentLength = Long.parseLong(response.headers().get(HEADER_CONTENT_LENGTH));
                }

                if (response.status().code() > 299) {
                    ReferenceCountUtil.release(msg);
                    exceptionCaught(ctx, new IOException("Failure response " + response.toString()));
                }

                if (contentLength == 0) {
                    emitter.onNext(new EmptyByteBuf(ByteBufAllocator.DEFAULT));
                    emitter.onCompleted();
                    adapter.pool.release(ctx.channel());
                }
            }
            if(msg instanceof HttpContent)
            {
                if (contentLength == 0) {
                    return;
                }

                HttpContent content = (HttpContent)msg;
                ByteBuf buf = content.content();

                if (contentLength > 0 && buf != null && buf.readableBytes() > 0) {
                    int readable = buf.readableBytes();
                    contentLength -= readable;
                    emitter.onNext(buf);
                }

                if (contentLength == 0) {
                    emitter.onCompleted();
                    adapter.pool.release(ctx.channel());
                }
            }
        }
    }

    private static class RetryChannelHandler extends ChannelDuplexHandler {
        private final NettyRxAdapter adapter;

        public RetryChannelHandler(NettyRxAdapter adapter) {
            this.adapter = adapter;
        }

        private void replay(ChannelHandlerContext ctx, int waitInSeconds) {
            ctx.executor().schedule(() -> {
                try {
                    ctx.channel().writeAndFlush(ctx.channel().attr(REQUEST_PROVIDER).get().provide());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }, waitInSeconds, TimeUnit.SECONDS);
        }

        private void resend(ChannelHandlerContext ctx, int waitInSeconds) {
            System.out.println("Resend Throttling for " + waitInSeconds + "s for channel " + ctx.channel().id().asShortText() + " on thread " + Thread.currentThread().getName() + " with range " + ctx.channel().attr(RANGE).get());
            Emitter<ByteBuf> emitter = ctx.channel().pipeline().get(HttpClientInboundHandler.class).emitter;
            DefaultHttpRequestProvider request = ctx.channel().attr(REQUEST_PROVIDER).get();
            adapter.pool.release(ctx.channel());

            ctx.executor().schedule(() -> {
                Future<Channel> future = adapter.pool.acquire();
                future.addListener(cf -> {
                    if (!cf.isSuccess()) {
                        emitter.onError(cf.cause());
                        return;
                    }
                    Channel channel = (Channel) cf.getNow();
                    channel.attr(REQUEST_PROVIDER).set(request);
                    channel.pipeline().get(HttpClientInboundHandler.class).setEmitter(emitter);
                    channel.writeAndFlush(request.provide());
                });
            }, waitInSeconds, TimeUnit.SECONDS);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            Integer retryCount = ctx.channel().attr(RETRY_COUNT).get();
            if (retryCount == null) {
                retryCount = 0;
            } else {
                retryCount++;
            }
            if (retryCount > 10) {
                super.exceptionCaught(ctx, cause);
            }
            ctx.channel().attr(RETRY_COUNT).set(retryCount);
            Integer finalRetryCount = retryCount;
            resend(ctx, (int) Math.pow(2, finalRetryCount));
        }

        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
            // not throttled, or the channel is marked authoring the failed request
            super.write(ctx, msg, promise);
        }

        @Override
        public void channelRead(final ChannelHandlerContext ctx, Object msg) throws Exception {
            if (msg instanceof HttpResponse) {
                HttpResponse res = (HttpResponse)msg;
                Integer retryCount = ctx.channel().attr(RETRY_COUNT).get();
                if (retryCount == null) {
                    retryCount = 1;
                } else {
                    retryCount++;
                }
                if (res.status().code() == 503 || res.status().code() == 500) {
                    if (retryCount > 10) {
                        super.exceptionCaught(ctx, new Exception("Retry max reached - " + res.toString()));
                    }
                    ctx.channel().attr(RETRY_COUNT).set(retryCount);
                    replay(ctx, (int) Math.pow(2, retryCount));
                } else {
                    super.channelRead(ctx, msg);
                }
            } else {
                super.channelRead(ctx, msg);
            }
        }
    }
}
