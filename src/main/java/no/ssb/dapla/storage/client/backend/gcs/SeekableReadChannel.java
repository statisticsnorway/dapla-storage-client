package no.ssb.dapla.storage.client.backend.gcs;

import com.google.cloud.ReadChannel;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.SeekableByteChannel;
import java.util.Map;
import java.util.TreeMap;

/**
 * Implements {@link SeekableByteChannel} over {@link ReadChannel}.
 */
public class SeekableReadChannel implements SeekableByteChannel {

    private final Map<Long, ByteBuffer> buffers = new TreeMap<>();
    private final ReadChannel delegate;
    private final Integer chunkSize;
    private final long size;
    private long pos;


    public SeekableReadChannel(ReadChannel delegate, Integer chunkSize, Long size) {
        this.chunkSize = chunkSize;
        this.delegate = delegate;
        this.delegate.setChunkSize(chunkSize);
        this.size = size;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        ByteBuffer chunk = buffer(position());
        chunk.limit(Math.min(dst.remaining(), chunk.remaining()));
        dst.put(chunk);
        pos += chunk.position();
        return pos > size ? -1 : chunk.position();
    }

    private Long chunkPosition(long pos) {
        return (pos / chunkSize) * chunkSize;
    }

    private ByteBuffer buffer(long position) throws IOException {
        Long chunkStart = chunkPosition(position);
        ByteBuffer buffer;
        if (!buffers.containsKey(chunkStart)) {
            buffer = ByteBuffer.allocate(chunkSize);
            delegate.seek(chunkStart);
            while (buffer.hasRemaining()) {
                int read = delegate.read(buffer);
                if (read < 0) {
                    break;
                }
            }
            buffer.limit(buffer.position());
            buffers.put(chunkStart, buffer);
        } else {
            buffer = buffers.get(chunkStart);
        }
        ByteBuffer copy = buffer.duplicate();
        copy.position(Math.toIntExact(position - chunkStart));
        return copy.slice();
    }

    @Override
    public int write(ByteBuffer src) {
        throw new NonWritableChannelException();
    }

    @Override
    public long position() {
        return pos;
    }

    @Override
    public SeekableByteChannel position(long newPosition) {
        pos = newPosition;
        return this;
    }

    @Override
    public long size() {
        return size;
    }

    @Override
    public SeekableByteChannel truncate(long size) {
        throw new NonWritableChannelException();
    }

    @Override
    public boolean isOpen() {
        return delegate.isOpen();
    }

    @Override
    public void close() {
        delegate.close();
    }
}
