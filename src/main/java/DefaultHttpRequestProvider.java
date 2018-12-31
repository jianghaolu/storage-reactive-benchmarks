import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by jianghlu on 9/5/2017.
 */
public class DefaultHttpRequestProvider {
    private final HttpVersion httpVersion;
    private final HttpMethod method;
    private final String uri;
    private final ByteBufSource source;

    public ByteBufSource source() {
        return source;
    }

    public String uri() {
        return uri;
    }

    public long capacity() {
        if (source == null) {
            return 0;
        } else {
            return source.capacity();
        }
    }

    private Map<String, String> headers;

    DefaultHttpRequestProvider(HttpVersion httpVersion, HttpMethod method, String uri) {
        this(httpVersion, method, uri, null);
    }

    DefaultHttpRequestProvider(HttpVersion httpVersion, HttpMethod method, String uri, ByteBufSource source) {
        this.httpVersion = httpVersion;
        this.method = method;
        this.uri = uri;
        this.source = source;
    }

    public String header(String key) {
        return headers.get(key);
    }

    public void setHeader(String key, String value) {
        if (headers == null) {
            headers = new HashMap<>();
        }
        headers.put(key, value);
    }

    public FullHttpRequest provide() throws IOException {
        DefaultFullHttpRequest request;
        if (source == null) {
            request = new DefaultFullHttpRequest(httpVersion, method, uri);
        } else {
            request = new DefaultFullHttpRequest(httpVersion, method, uri, source.read());
        }
        for (Map.Entry<String, String> header : headers.entrySet()) {
            request.headers().add(header.getKey(), header.getValue());
        }
        return request;
    }
}
