package no.ssb.dapla.storage.client.backend.hadoop;

import io.reactivex.Flowable;
import no.ssb.dapla.storage.client.backend.BinaryBackend;
import org.apache.hadoop.fs.ByteBufferReadable;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;

public class HadoopBackend implements BinaryBackend {

    private final FileSystem fileSystem;

    public HadoopBackend(FileSystem fileSystem) {
        this.fileSystem = fileSystem;
    }

    @Override
    public Flowable<String> list(String path) throws IOException {
        return null;
    }

    @Override
    public SeekableByteChannel read(String path) throws IOException {
        Path fsPath = new Path(path);
        FSDataInputStream dataInputStream = fileSystem.open(fsPath);
        return new SeekableByteChannel() {
            @Override
            public int read(ByteBuffer dst) throws IOException {
                if (dataInputStream.getWrappedStream() instanceof ByteBufferReadable) {
                    return dataInputStream.read(dst);
                } else {
                    return dataInputStream.read(dst.array(), dst.position(), dst.remaining());
                }
            }

            @Override
            public int write(ByteBuffer src) throws IOException {
                throw new IOException("not writable");
            }

            @Override
            public long position() throws IOException {
                return dataInputStream.getPos();
            }

            @Override
            public SeekableByteChannel position(long newPosition) throws IOException {
                dataInputStream.seek(newPosition);
                return this;
            }

            @Override
            public long size() throws IOException {
                return fileSystem.getFileStatus(fsPath).getLen();
            }

            @Override
            public SeekableByteChannel truncate(long size) throws IOException {
                throw new IOException("not writable");
            }

            @Override
            public boolean isOpen() {
                return true;
            }

            @Override
            public void close() throws IOException {
                dataInputStream.close();
            }
        };
    }

    @Override
    public SeekableByteChannel write(String path) throws IOException {
        Path fsPath = new Path(path);
        FSDataOutputStream dataOutputStream = fileSystem.create(fsPath);
        return new SeekableByteChannel() {
            @Override
            public int read(ByteBuffer dst) throws IOException {
                throw new IOException("not readable");
            }

            @Override
            public int write(ByteBuffer src) throws IOException {
                int len = src.remaining();
                dataOutputStream.write(src.array(), src.position(), len);
                src.position(src.position() + src.remaining());
                return len;
            }

            @Override
            public long position() {
                return dataOutputStream.getPos();
            }

            @Override
            public SeekableByteChannel position(long newPosition) throws IOException {
                throw new IOException("not implemented");
            }

            @Override
            public long size() throws IOException {
                return fileSystem.getFileStatus(fsPath).getLen();
            }

            @Override
            public SeekableByteChannel truncate(long size) throws IOException {
                throw new IOException("not implemented");
            }

            @Override
            public boolean isOpen() {
                return true;
            }

            @Override
            public void close() throws IOException {
                dataOutputStream.close();
            }
        };
    }

    @Override
    public void move(String from, String to) throws IOException {
        fileSystem.rename(new Path(from), new Path(to));
    }

    @Override
    public void delete(String path) throws IOException {
        if (!fileSystem.delete(new Path(path), false)) {
            throw new IOException("Could not delete " + path);
        }
    }
}
