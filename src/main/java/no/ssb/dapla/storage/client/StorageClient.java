package no.ssb.dapla.storage.client;

import com.google.api.client.util.Lists;
import io.reactivex.Completable;
import io.reactivex.Flowable;
import io.reactivex.Observable;
import no.ssb.dapla.storage.client.backend.BinaryBackend;
import no.ssb.dapla.storage.client.backend.FileInfo;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * Data client is an abstraction to read and write Parquet files on bucket storage.
 */
public class StorageClient {

    private final BinaryBackend backend;
    private final ParquetProvider provider;
    private final String location;

    private StorageClient(Builder builder) {
        this.backend = Objects.requireNonNull(builder.binaryBackend);
        this.provider = Objects.requireNonNull(builder.parquetProvider);
        this.location = Objects.requireNonNull(builder.location);
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Write an unbounded sequence of {@link GenericRecord}s to the bucket storage.
     * <p>
     * The records will be written in "batches" of size count or when the timespan duration elapsed. The last value of
     * each batch is returned in an {@link Observable}.
     *
     * @param idSupplier  a supplier for the id called each time a file is flushed.
     * @param records     the records to write.
     * @param timeWindow  the period of time before a batch should be written.
     * @param unit        the unit of time that applies to the timespan argument.
     * @param countWindow the maximum size of a batch before it should be written.
     * @return an {@link Observable} emitting the last record in each batch.
     */
    public <R extends GenericRecord> Observable<R> writeDataUnbounded(
            Supplier<String> idSupplier, Schema schema, Flowable<R> records, long timeWindow, TimeUnit unit, long countWindow
    ) {
        return records
                .window(timeWindow, unit, countWindow, true)
                .switchMapMaybe(
                        recordsWindow -> writeData(idSupplier.get(), schema, recordsWindow).lastElement()
                )
                .toObservable();
    }

    /**
     * Write a sequence of {@link GenericRecord}s to the bucket storage.
     *
     * @param dataId  an opaque identifier for the data.
     * @param schema  the schema used to create the records.
     * @param records the records to write.
     * @return a completable that completes once the data is saved.
     */
    public Completable writeAllData(String dataId, Schema schema, Flowable<GenericRecord> records) {
        return writeData(dataId, schema, records).ignoreElements();
    }

    /**
     * Write a sequence of {@link GenericRecord}s to the bucket storage.
     *
     * @param dataId  an opaque identifier for the data.
     * @param schema  the schema used to create the records.
     * @param records the records to write.
     * @return a completable that completes once the data is saved.
     */
    public <R extends GenericRecord> Flowable<R> writeData(String dataId, Schema schema, Flowable<R> records) {
        return Flowable.defer(() -> {
            DataWriter writer = new DataWriter(dataId, schema);
            return records
                    .doAfterNext(writer::write)
                    .doOnComplete(writer::close)
                    .doOnError(throwable -> writer.cancel());
        });
    }

    /**
     * Read a sequence of {@link GenericRecord}s from the bucket storage.
     *
     * @param dataId the identifier for the data.
     * @param cursor a cursor on record number.
     * @return a {@link Flowable} of records.
     */
    public Flowable<GenericRecord> readData(String dataId, Cursor<Long> cursor) {
        // TODO: Handle projection.
        // TODO: Handle filtering.
        if (cursor != null) {
            // Convert to pos + size
            long start = Math.max(cursor.getAfter(), 0);
            int size = Math.max(cursor.getNext(), 0);
            // Note the size + 1 here. The filter implementation goes through all the groups unless
            // we return one extra and limit with actual size. This will probably be fixed by parquet team at some
            // point.
            FilterCompat.Filter filter = FilterCompat.get(new PagedRecordFilter(start, start + size + 1));
            return readRecords(dataId, filter).limit(size);
        } else {
            return readRecords(dataId, FilterCompat.NOOP);
        }
    }

    private Flowable<GenericRecord> readRecords(String dataId, FilterCompat.Filter filter) {
        return Flowable.generate(() -> {
            SeekableByteChannel readableChannel = backend.read(pathTo(dataId));
            return provider.getReader(readableChannel, filter);
        }, (parquetReader, emitter) -> {
            GenericRecord read = parquetReader.read();
            if (read == null) {
                emitter.onComplete();
            } else {
                emitter.onNext(read);
            }
        }, parquetReader -> {
            parquetReader.close();
        });
    }

    /**
     * List the files for a given data id.
     *
     * @param dataId the data identifier.
     * @return the files as a List of {@link FileInfo}.
     */
    public List<FileInfo> list(String dataId) {
        try {
            return Lists.newArrayList(backend.list(pathTo(dataId)).blockingIterable());
        } catch (IOException e) {
            throw new StorageClientException(String.format("Unable to list files in path '%s'", pathTo(dataId)), e);
        }
    }

    /**
     * Find the file that was most recently modified, ignoring directories and files with a '.tmp' suffix.
     *
     * @param dataId the data identifier.
     * @return the file as a {@link FileInfo}.
     */
    public FileInfo getLastModified(String dataId) {
        try {
            return backend
                    .list(pathTo(dataId), Comparator.comparing(FileInfo::getLastModified))
                    .filter(fileInfo -> !fileInfo.isDirectory() && !fileInfo.hasSuffix(".tmp"))
                    .lastElement()
                    .blockingGet();
        } catch (IOException e) {
            throw new StorageClientException(String.format("Unable to find last modified in path '%s'", pathTo(dataId)), e);
        }
    }

    /**
     * Read a parquet file with a given data identifier, using a given projection schema. The projection schema is a
     * subset of the complete original schema for the file, and only the columns needed to reconstruct the projection
     * schema will be scanned. E.g:
     * <p>Original schema:</p>
     * <pre>
     * {@code
     * message no.ssb.dataset.root {
     *   required binary string (UTF8);
     *   required int32 int;
     *   required boolean boolean;
     *   required float float;
     *   required int64 long;
     *   required double double;
     * }
     * }
     * </pre>
     * <p>If only interested in the 'int' column, the below projection schema would save you from having to scan all columns:</p>
     * <pre>
     * {@code
     * message no.ssb.dataset.root {
     *   required int32 int;
     * }
     * }
     * </pre>
     *
     * @param dataId           the data identifier.
     * @param projectionSchema the projection schema.
     * @param groupVisitor     a {@link ParquetGroupVisitor} that will be applied to each record found.
     */
    public void readParquetFile(String dataId, MessageType projectionSchema, ParquetGroupVisitor groupVisitor) {
        String path = pathTo(dataId);
        try (
                SeekableByteChannel channel = backend.read(path);
                ParquetReader<Group> reader = provider.getParquetGroupReader(channel, projectionSchema.toString())
        ) {
            SimpleGroup group;
            while ((group = (SimpleGroup) reader.read()) != null) {
                groupVisitor.visit(group);
            }
        } catch (Exception e) {
            throw new StorageClientException(String.format("Failed to read parquet file in path '%s'", path), e);
        }
    }

    /**
     * Return the full path to data, including location
     */
    protected String pathTo(String dataId) {
        String rootPath = location.replaceFirst("/*$", "");
        return dataId.startsWith(rootPath) ? dataId : rootPath + "/" + dataId;
    }

    public ParquetMetadata readMetadata(String dataId) throws IOException {
        try (SeekableByteChannel channel = backend.read(pathTo(dataId))) {
            ParquetFileReader parquetFileReader = provider.getParquetFileReader(channel);
            return parquetFileReader.getFooter();
        }
    }

    public static class Builder {

        private ParquetProvider parquetProvider;
        private BinaryBackend binaryBackend;
        public String location;


        public Builder withParquetProvider(ParquetProvider parquetProvider) {
            this.parquetProvider = parquetProvider;
            return this;
        }

        public Builder withBinaryBackend(BinaryBackend binaryBackend) {
            this.binaryBackend = binaryBackend;
            return this;
        }

        public Builder withLocation(String location) {
            this.location = location;
            return this;
        }

        public StorageClient build() {
            return new StorageClient(this);
        }

    }

    /**
     * Offers read, write and delete operations on records.
     */
    public class DataWriter implements AutoCloseable {
        private final String tmpPath;
        private final String path;
        private final ParquetWriter<GenericRecord> parquetWriter;
        private final AtomicInteger writeCounter = new AtomicInteger(0);

        private DataWriter(String datasetId, Schema schema) throws IOException {
            path = pathTo(datasetId);
            tmpPath = path + ".tmp";
            SeekableByteChannel channel = backend.write(tmpPath);
            parquetWriter = provider.getWriter(channel, schema);
        }

        /**
         * Write a record to storage as a parquet file.
         * <p>
         * Note: The record might be buffered. {@link #close()} must be called afterwards to ensure all buffered records
         * are persisted.
         *
         * @param record the record to save.
         */
        public void write(GenericRecord record) throws IOException {
            writeCounter.incrementAndGet();
            parquetWriter.write(record);
        }

        public void cancel() throws IOException {
            try {
                parquetWriter.close();
            } finally {
                backend.delete(tmpPath);
            }
        }

        /**
         * Write all buffered records.
         */
        @Override
        public void close() throws IOException {
            try {
                parquetWriter.close();
                if (writeCounter.get() > 0) {
                    // Remove .tmp suffix from the file since at least one record has been written to it
                    backend.move(tmpPath, path);
                } else {
                    // Delete the file since no records have been written to it
                    backend.delete(tmpPath);
                    writeCounter.set(0);
                }

            } catch (IOException ioe) {
                try {
                    cancel();
                } catch (IOException deleteIoe) {
                    ioe.addSuppressed(deleteIoe);
                }
                throw ioe;
            }
        }
    }

    public static class StorageClientException extends RuntimeException {
        public StorageClientException(String message, Throwable cause) {
            super(message, cause);
        }

        public StorageClientException(String message) {
            super(message);
        }
    }
}
