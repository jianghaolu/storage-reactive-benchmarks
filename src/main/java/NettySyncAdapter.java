import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequestEncoder;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseDecoder;
import io.netty.handler.proxy.HttpProxyHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;

import javax.net.ssl.SSLException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by jianghlu on 7/20/17.
 */
public class NettySyncAdapter {
    private Bootstrap bootstrap;
    private SslContext sslContext;

    public NettySyncAdapter() {
        bootstrap = new Bootstrap();
        bootstrap.group(new NioEventLoopGroup());
        bootstrap.channel(NioSocketChannel.class);
        bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
        try {
            sslContext = SslContextBuilder.forClient()
                    .trustManager(InsecureTrustManagerFactory.INSTANCE).build();
        } catch (SSLException e) {
            e.printStackTrace();
        }
        bootstrap.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws Exception {
                if (sslContext != null) {
                    ch.pipeline().addLast(sslContext.newHandler(ch.alloc(), "https://sdkbenchmark.blob.core.windows.net", 443));
                }
                ch.pipeline().addLast(new HttpResponseDecoder());
                ch.pipeline().addLast(new HttpRequestEncoder());
                ch.pipeline().addLast(new HttpClientInboundHandler());
                ch.pipeline().addFirst(new HttpProxyHandler(new InetSocketAddress("localhost", 8888)));
            }
        });
    }

    public ListenableFuture<ByteBuf> sendRequest(DefaultFullHttpRequest request) {
        try {
            URI uri = new URI(request.uri());
            ChannelFuture future = bootstrap.connect(uri.getHost(), uri.getPort()).sync();

            request.headers().set(HttpHeaders.Names.HOST, uri.getHost());
            request.headers().set(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
            request.headers().set(HttpHeaders.Names.CONTENT_LENGTH, request.content().readableBytes());

            future.channel().writeAndFlush(request).addListener(f -> {
                System.out.println("writeAndFlush listener");
                future.channel().closeFuture();
            });
            System.out.println("After writeAndFlush");
            HttpClientInboundHandler handler = (HttpClientInboundHandler) future.channel().pipeline().last();
            return handler.future;

        } catch (URISyntaxException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static class HttpClientInboundHandler extends ChannelInboundHandlerAdapter {

        private static final String HEADER_CONTENT_LENGTH = "Content-Length";

        private CompositeByteBuf compositeByteBuf = ByteBufAllocator.DEFAULT.compositeBuffer();

        private long contentLength;

        private AdaptedAbstractFuture<ByteBuf> future = new AdaptedAbstractFuture<ByteBuf>();

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (msg instanceof HttpResponse)
            {
                System.out.println("Incoming response");
                HttpResponse response = (HttpResponse) msg;
                if (response.headers().contains(HEADER_CONTENT_LENGTH)) {
                    contentLength = Long.parseLong(response.headers().get(HEADER_CONTENT_LENGTH));
                }
            }
            if(msg instanceof HttpContent)
            {
                HttpContent content = (HttpContent)msg;
                ByteBuf buf = content.content();
                if (contentLength != 0 && buf != null && buf.readableBytes() > 0) {
                    contentLength -= buf.readableBytes();
                    compositeByteBuf.addComponent(buf);
                }

                if (contentLength == 0) {
                    future.done(compositeByteBuf);
                }
            }
        }
    }

    private static class AdaptedAbstractFuture<T> extends AbstractFuture<T> {
        public void done(T t) {
            set(t);
        }
    }
}
