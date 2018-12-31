import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.nio.channels.FileChannel;

/**
 * Created by jianghlu on 9/5/2017.
 */
public class ByteArrayByteBufSource implements ByteBufSource {
    private final byte[] bytes;

    public long length() {
        return bytes.length;
    }

    public long capacity() {
        return bytes.length;
    }

    public ByteArrayByteBufSource(byte[] bytes) {
        this.bytes = bytes;
    }

    public ByteBuf read() throws IOException {
        return Unpooled.wrappedBuffer(bytes);
    }
}
