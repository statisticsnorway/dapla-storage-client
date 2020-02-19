package no.ssb.dapla.storage.client.backend.gcs;

import com.google.api.gax.paging.Page;
import com.google.cloud.ReadChannel;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.CopyWriter;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import io.reactivex.Flowable;
import no.ssb.dapla.storage.client.BinaryBackend;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.Comparator;

/**
 * A simple BinaryBackend for Google Cloud Storage.
 */
public class GoogleCloudStorageBackend implements BinaryBackend {

    private final Storage storage;
    private final Integer writeChunkSize;
    private final Integer readChunkSize;

    public GoogleCloudStorageBackend(Configuration configuration) {
        this.storage = StorageOptions.getDefaultInstance().getService();
        this.writeChunkSize = configuration.getWriteChunkSize();
        this.readChunkSize = configuration.getReadChunkSize();
    }

    private static String fuse(String start, String end) {
        for (int i = 0; i < start.length(); i++) {
            if (end.startsWith(start.substring(i))) {
                return start.substring(0, i) + end;
            }
        }
        return start + end;
    }

    @Override
    public Flowable<String> list(String path) throws IOException {
        BlobId id = getBlobId(path);
        return Flowable.defer(() -> {
            Page<Blob> pages = storage.list(id.getBucket(), Storage.BlobListOption.prefix(id.getName()));
            return Flowable.fromIterable(pages.iterateAll());
        }).map(blob -> String.format("gs://%s/%s",blob.getBucket(), blob.getName())).sorted(Comparator.reverseOrder());
    }

    @Override
    public SeekableByteChannel read(String path) throws IOException {
        Blob blob = storage.get(getBlobId(path));
        ReadChannel reader = blob.reader();
        reader.setChunkSize(readChunkSize);
        return new SeekableReadChannel(reader, readChunkSize, blob.getSize());
    }

    @Override
    public SeekableByteChannel write(String path) throws IOException {
        Blob blob = storage.create(BlobInfo.newBuilder(getBlobId(path)).build());
        WriteChannel writer = blob.writer();
        writer.setChunkSize(writeChunkSize);
        return new SeekableByteChannel() {

            long pos = 0;

            @Override
            public int read(ByteBuffer dst) {
                throw new UnsupportedOperationException("not readable");
            }

            @Override
            public int write(ByteBuffer src) throws IOException {
                int written = writer.write(src);
                pos += written;
                return written;
            }

            @Override
            public long position() {
                return pos;
            }

            @Override
            public SeekableByteChannel position(long newPosition) {
                throw new UnsupportedOperationException("not seekable");
            }

            @Override
            public long size() {
                return position();
            }

            @Override
            public SeekableByteChannel truncate(long size) {
                throw new UnsupportedOperationException("truncate not supported");
            }

            @Override
            public boolean isOpen() {
                return true;
            }

            @Override
            public void close() throws IOException {
                writer.close();
            }
        };
    }

    @Override
    public void move(String from, String to) throws IOException {
        Blob fromBlob = storage.get(getBlobId(from));
        CopyWriter copyWriter = fromBlob.copyTo(getBlobId(to));
        copyWriter.getResult(); // So we block.
        fromBlob.delete();
    }

    @Override
    public void delete(String path) throws IOException {
        boolean delete = storage.delete(getBlobId(path));
        if (!delete) {
            throw new FileNotFoundException(path);
        }
    }

    private BlobId getBlobId(String path) throws IOException {
        try {
            URI uri = new URI(path);
            String bucket = uri.getHost();
            String name = uri.getPath();
            if (name.startsWith("/")) {
                name = name.substring(1);
            }
            return BlobId.of(bucket, name);
        } catch (URISyntaxException use) {
            throw new IOException("could not get bucket and name from " + path);
        }
    }

    public static class Configuration {

        private Integer readChunkSize;
        private Integer writeChunkSize;

        public Configuration() {
        }

        public Integer getReadChunkSize() {
            return readChunkSize;
        }

        public void setReadChunkSize(Integer readChunkSize) {
            this.readChunkSize = readChunkSize;
        }

        public Integer getWriteChunkSize() {
            return writeChunkSize;
        }

        public void setWriteChunkSize(Integer writeChunkSize) {
            this.writeChunkSize = writeChunkSize;
        }
    }
}
