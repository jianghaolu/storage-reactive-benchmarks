import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.io.IOException;
import java.nio.channels.FileChannel;

/**
 * Created by jianghlu on 9/5/2017.
 */
public class FileByteBufSource implements ByteBufSource {
    private final FileChannel fileChannel;
    private final long start;
    private final long length;
    private final long capacity;
    private final ByteBufAllocator allocator;
    private int readCount = 0;
    private ByteBuf cached;

    public long length() {
        return length;
    }

    public long capacity() {
        return capacity;
    }

    public FileByteBufSource(FileChannel fileChannel, long start, long length, ByteBufAllocator allocator) {
        this(fileChannel, start, length, length, allocator);
    }

    public FileByteBufSource(FileChannel fileChannel, long start, long length, long capacity, ByteBufAllocator allocator) {
        if (fileChannel == null || !fileChannel.isOpen()) {
            throw new IllegalArgumentException("File channel is null or closed.");
        }
        try {
            if (start + length > fileChannel.size()) {
                throw new IndexOutOfBoundsException("Position " + start + " + length " + length + " but file size " + fileChannel.size());
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("Unable to read from file.", e);
        }
        this.fileChannel = fileChannel;
        this.start = start;
        this.length = length;
        this.capacity = capacity;
        this.allocator = allocator;
    }

    public ByteBuf read() throws IOException {
        if (cached != null) {
            ByteBuf ret = cached;
            cached = null;
            return ret;
        }
        return readRaw();
    }

    private ByteBuf readRaw() throws IOException {
        ++readCount;
        ByteBuf direct = allocator.buffer((int) capacity, (int) capacity);
        direct.writeBytes(fileChannel, start, (int) length);
        direct.writeZero((int) (capacity - length));
        return direct;
    }
}
