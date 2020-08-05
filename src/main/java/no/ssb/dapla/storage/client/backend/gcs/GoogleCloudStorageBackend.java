package no.ssb.dapla.storage.client.backend.gcs;

import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.ComputeEngineCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.ReadChannel;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.CopyWriter;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import io.reactivex.Completable;
import io.reactivex.Flowable;
import io.reactivex.Single;
import no.ssb.dapla.storage.client.backend.BinaryBackend;
import no.ssb.dapla.storage.client.backend.FileInfo;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A simple BinaryBackend for Google Cloud Storage.
 */
public class GoogleCloudStorageBackend implements BinaryBackend {

    private final Storage storage;
    private final Integer writeChunkSize;
    private final Integer readChunkSize;

    public GoogleCloudStorageBackend() {
        this(new Configuration());
    }

    public GoogleCloudStorageBackend(Configuration configuration) {
        Objects.requireNonNull(configuration);
        this.writeChunkSize = configuration.getWriteChunkSize();
        this.readChunkSize = configuration.getReadChunkSize();
        Path serviceAccountCredentials = configuration.getServiceAccountCredentials();

        GoogleCredentials credentials;
        if (serviceAccountCredentials != null) {
            try {
                credentials = ServiceAccountCredentials.fromStream(Files.newInputStream(serviceAccountCredentials, StandardOpenOption.READ));
            } catch (IOException e) {
                throw new RuntimeException(String.format("could not read service account credentials from path %s", serviceAccountCredentials.toString()), e);
            }
        } else {
            credentials = ComputeEngineCredentials.create();
        }
        credentials = credentials.createScoped(List.of("https://www.googleapis.com/auth/devstorage.read_write"));

        this.storage = StorageOptions.newBuilder()
                .setCredentials(credentials)
                .build()
                .getService();
    }

    public Flowable<FileInfo> list(String path, Comparator<FileInfo>... comparators) throws IOException {
        BlobId id = getBlobId(path);
        Comparator<FileInfo> comparator = List.of(comparators).stream()
                .reduce(Comparator::thenComparing)
                .orElse(Comparator.comparing(FileInfo::getPath));

        return Flowable.defer(() -> {
            Page<Blob> pages = storage.list(id.getBucket(), Storage.BlobListOption.prefix(id.getName()));
            return Flowable.fromIterable(pages.iterateAll());
        })
                .map(blob -> {
                    String blobName = blob.getName();
                    String[] segments = blobName.split("/");
                    String filename = segments[segments.length - 1];
                    String filePath = String.format("gs://%s/%s", blob.getBucket(), blobName);
                    return new FileInfo(filename, filePath, blob.getUpdateTime(), blob.isDirectory());
                })
                .sorted(comparator);
    }

    @Override
    public SeekableByteChannel read(String path) throws IOException {
        Blob blob = storage.get(getBlobId(path));
        if (blob == null) {
            throw new NullPointerException(String.format("could not find blob in path '%s'", path));
        }
        ReadChannel reader = blob.reader();
        reader.setChunkSize(readChunkSize);
        return new SeekableReadChannel(reader, readChunkSize, blob.getSize());
    }

    /**
     * Write a stream of bytes to a blob.
     * <p/>
     * Example: Upload the contents of the file <code>file-to-upload</code> to the blob <code>gs://my-bucket/my-file</code>
     * <pre>
     * <code>
     *
     * GoogleCloudStorageBackend backend = new GoogleCloudStorageBackend();
     *
     * try (InputStream is = Files.newInputStream(Path.of("file-to-upload"))) {
     *   backend.write("gs://my-bucket/my-file", is);
     * }
     * </code>
     * </pre>
     * @param path    a full path to the blob, including schema, e.g <i>gs://my-bucket/my-file</i>
     * @param content a stream of bytes as a {@link InputStream}
     * @throws IOException if an I/O error occurs
     */
    public void write(String path, InputStream content) throws IOException {
        BlobInfo blobInfo = BlobInfo.newBuilder(getBlobId(path)).setContentType("text/plain").build();
        try (WriteChannel channel = storage.writer(blobInfo)) {
            byte[] buffer = new byte[8192];
            int read;
            while ((read = content.read(buffer)) != -1) {
                channel.write(ByteBuffer.wrap(buffer, 0, read));
                buffer = new byte[8192];
            }
        }
    }

    /**
     * Asynchronously write a stream of byte arrays to a blob.
     * <p/>
     * Example: Upload the contents of the file <code>file-to-upload</code> to the blob <code>gs://my-bucket/my-file</code>
     * <pre>
     * <code>
     *
     * GoogleCloudStorageBackend backend = new GoogleCloudStorageBackend();
     *
     * try (InputStream is = Files.newInputStream(Path.of("file-to-upload"))) {
     *
     *   backend
     *     .write("gs://my-bucket/my-file", Bytes.from(is))
     *     .timeout(10, TimeUnit.SECONDS);
     *     .blockingAwait();
     * }
     * </code>
     * </pre>
     *
     * @param path    a full path to the blob, including schema, e.g <i>gs://my-bucket/my-file</i>
     * @param content the stream as a {@link Flowable} of {@code byte[]}
     * @return {@link Completable#complete()} if successful, {@link Completable#error(Throwable)} otherwise
     */
    public Completable write(String path, Flowable<byte[]> content) {
        return Single
                .fromCallable(() -> BlobInfo.newBuilder(getBlobId(path)).setContentType("text/plain").build())
                .flatMapCompletable(blobInfo -> {
                    final AtomicReference<Throwable> error = new AtomicReference<>(null);
                    try (WriteChannel channel = storage.writer(blobInfo)) {
                        content
                                .subscribe(
                                        bytes -> channel.write(ByteBuffer.wrap(bytes, 0, bytes.length)),
                                        error::set
                                );
                    }
                    Throwable throwable = error.get();
                    if (throwable != null) {
                        return Completable.error(throwable);
                    }
                    return Completable.complete();
                });
    }

    @Override
    public void write(String path, byte[] content) throws IOException {
        BlobInfo blobInfo = BlobInfo.newBuilder(getBlobId(path)).setContentType("text/plain").build();
        try (WriteChannel writer = storage.writer(blobInfo)) {
            writer.write(ByteBuffer.wrap(content, 0, content.length));
        } catch (Exception e) {
            throw new RuntimeException(String.format("failed to write to path %s", path), e);
        }
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
        copyWriter.getResult();
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

        private Integer readChunkSize = 128 * 1024 * 1024;
        private Integer writeChunkSize = 128 * 1024 * 1024;
        private Path serviceAccountCredentials = null;

        public Integer getReadChunkSize() {
            return readChunkSize;
        }

        public Configuration setReadChunkSize(Integer readChunkSize) {
            this.readChunkSize = Objects.requireNonNull(readChunkSize);
            return this;
        }

        public Integer getWriteChunkSize() {
            return writeChunkSize;
        }

        public Configuration setWriteChunkSize(Integer writeChunkSize) {
            this.writeChunkSize = Objects.requireNonNull(writeChunkSize);
            return this;
        }

        public Path getServiceAccountCredentials() {
            return serviceAccountCredentials;
        }

        public Configuration setServiceAccountCredentials(Path serviceAccountCredentials) {
            this.serviceAccountCredentials = serviceAccountCredentials;
            return this;
        }
    }
}
