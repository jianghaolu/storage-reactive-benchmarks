import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.FullHttpRequest;

import java.io.IOException;

/**
 * Created by jianghlu on 9/5/2017.
 */
public interface ByteBufSource {
    long length();
    long capacity();
    ByteBuf read() throws IOException;
}
