import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequestEncoder;
import io.netty.handler.codec.http.HttpResponseDecoder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import rx.Observable;

import javax.net.ssl.SSLException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by jianghlu on 7/20/17.
 */
public class NettyDummyAdapter {
    private Bootstrap bootstrap;
    private SslContext sslContext;

    public NettyDummyAdapter() {
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
            }
        });
    }

    public Observable<ByteBuf> sendRequestAsync(DefaultFullHttpRequest request) {
            return Observable.defer(() -> {
                try {
                    URI uri = new URI(request.uri());

                    request.headers().set(HttpHeaders.Names.HOST, uri.getHost());
                    request.headers().set(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
                    request.headers().set(HttpHeaders.Names.CONTENT_LENGTH, request.content().readableBytes());

                    request.release(1);

                    return Observable.just(null);
                } catch (URISyntaxException e) {
                    throw new RuntimeException(e);
                }
            });
    }

    public ListenableFuture<ByteBuf> sendRequestFuture(DefaultFullHttpRequest request) {
        try {
            URI uri = new URI(request.uri());

            request.headers().set(HttpHeaders.Names.HOST, uri.getHost());
            request.headers().set(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
            request.headers().set(HttpHeaders.Names.CONTENT_LENGTH, request.content().readableBytes());

            request.release(1);

            SettableFuture<ByteBuf> ret = SettableFuture.create();
            ret.set(null);
            return ret;
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public static class HttpClientInboundHandler extends ChannelInboundHandlerAdapter {

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        }
    }
}
