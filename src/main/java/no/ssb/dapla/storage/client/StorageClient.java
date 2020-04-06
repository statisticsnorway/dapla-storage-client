package no.ssb.dapla.storage.client;

import com.google.api.client.util.Lists;
import io.reactivex.Completable;
import io.reactivex.Flowable;
import io.reactivex.Observable;
import no.ssb.dapla.dataset.uri.DatasetUri;
import no.ssb.dapla.storage.client.backend.BinaryBackend;
import no.ssb.dapla.storage.client.backend.FileInfo;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * StorageClient is an abstraction to read and write Parquet files on bucket storage.
 */
public class StorageClient {

    private final BinaryBackend backend;
    private final ParquetProvider provider;

    private StorageClient(Builder builder) {
        this.backend = Objects.requireNonNull(builder.binaryBackend);
        this.provider = Objects.requireNonNull(builder.parquetProvider);
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Write an unbounded sequence of {@link GenericRecord}s to a given dataset.
     * <p>
     * The records will be written in "batches" of size count or when the timespan duration elapsed. The last value of
     * each batch is returned in an {@link Observable}.
     * <p>
     *
     * @param datasetUri        the dataset to write to.
     * @param filenameGenerator a filename generator to invoke each time a file is flushed.
     * @param records           the records to write.
     * @param timeWindow        the period of time before a batch should be written.
     * @param unit              the unit of time that applies to the timespan argument.
     * @param countWindow       the maximum size of a batch before it should be written.
     * @return an {@link Observable} emitting the last record in each batch.
     */
    public <R extends GenericRecord> Observable<R> writeDataUnbounded(
            DatasetUri datasetUri, Supplier<String> filenameGenerator, Schema schema, Flowable<R> records, long timeWindow, TimeUnit unit, long countWindow
    ) {
        return records
                .window(timeWindow, unit, countWindow, true)
                .switchMapMaybe(
                        recordsWindow -> writeData(datasetUri, filenameGenerator.get(), schema, recordsWindow).lastElement()
                )
                .toObservable();
    }

    /**
     * Write a sequence of {@link GenericRecord}s to a given dataset.
     * <p>
     *
     * @param datasetUri the dataset to write to.
     * @param filename   the name of the file in which to write.
     * @param schema     the schema used to create the records.
     * @param records    the records to write.
     * @return a completable that completes once the data is saved.
     */
    public Completable writeAllData(DatasetUri datasetUri, String filename, Schema schema, Flowable<GenericRecord> records) {
        return writeData(datasetUri, filename, schema, records).ignoreElements();
    }

    /**
     * Write a sequence of {@link GenericRecord}s to a given dataset.
     * <p>
     *
     * @param datasetUri the dataset to write to.
     * @param filename   the name of the file in which to write.
     * @param schema     the schema used to create the records.
     * @param records    the records to write.
     * @return a completable that completes once the data is saved.
     */
    public <R extends GenericRecord> Flowable<R> writeData(DatasetUri datasetUri, String filename, Schema schema, Flowable<R> records) {
        return Flowable.defer(() -> {
            DataWriter writer = new DataWriter(datasetUri, filename, schema);
            return records
                    .doAfterNext(writer::write)
                    .doOnComplete(writer::close)
                    .doOnError(throwable -> writer.cancel());
        });
    }

    /**
     * List the files a given dataset is composed of.
     * <p>
     *
     * @param datasetUri the dataset.
     * @return the files as a List of {@link FileInfo}.
     */
    public List<FileInfo> listDatasetFiles(DatasetUri datasetUri) {
        return listDatasetFilesByLastModified(datasetUri);
    }

    /**
     * List the files a given dataset is composed of, in the order in which they were modified.
     * <p>
     *
     * @param datasetUri the dataset.
     * @return the files as a List of {@link FileInfo}.
     */
    public List<FileInfo> listDatasetFilesByLastModified(DatasetUri datasetUri) {
        try {
            return backend
                    .list(datasetUri.toString(), Comparator.comparing(FileInfo::getLastModified))
                    .filter(fileInfo -> !fileInfo.isDirectory() && fileInfo.hasSuffix(".parquet"))
                    .toList()
                    .blockingGet();
        } catch (IOException e) {
            throw new StorageClientException(String.format("Unable to list by last modified in path '%s'", datasetUri.toString()), e);
        }
    }

    /**
     * Of all the files used to compose a given dataset, get the one that was modified last.
     * <p>
     *
     * @param datasetUri the dataset.
     * @return the file as a {@link Optional<FileInfo>} if the dataset has one or more files, {@link Optional#empty()} otherwise.
     */
    public Optional<FileInfo> getLastModifiedDatasetFile(DatasetUri datasetUri) {
        List<FileInfo> files = listDatasetFilesByLastModified(datasetUri);
        if (files == null || files.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(files.get(files.size() - 1));
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
     * <p>
     *
     * @param datasetUri       the dataset to which the parquet file belongs.
     * @param filename         the name of the parquet file.
     * @param projectionSchema the projection schema.
     * @param groupVisitor     a {@link ParquetGroupVisitor} that will be applied to each record found.
     */
    public void readParquetFile(DatasetUri datasetUri, String filename, MessageType projectionSchema, ParquetGroupVisitor groupVisitor) {
        String filePath = resolveFilePath(datasetUri, filename);
        try (
                SeekableByteChannel channel = backend.read(filePath);
                ParquetReader<Group> reader = provider.getParquetGroupReader(channel, projectionSchema.toString())
        ) {
            SimpleGroup group;
            while ((group = (SimpleGroup) reader.read()) != null) {
                groupVisitor.visit(group);
            }
        } catch (Exception e) {
            throw new StorageClientException(String.format("Failed to read parquet file in path '%s'", filePath), e);
        }
    }

    /**
     * Resolve the full path to a dataset file.
     */
    private static String resolveFilePath(DatasetUri datasetUri, String filename) {
        String uri = datasetUri.toString();
        StringBuilder fileNameBuilder = new StringBuilder(uri);
        if (!uri.endsWith("/")) {
            fileNameBuilder.append("/");
        }
        fileNameBuilder.append(filename);
        if (!filename.endsWith(".parquet")) {
            fileNameBuilder.append(".parquet");
        }
        return fileNameBuilder.toString();
    }

    public static class Builder {

        private ParquetProvider parquetProvider = new ParquetProvider();
        private BinaryBackend binaryBackend;

        public Builder withParquetProvider(ParquetProvider parquetProvider) {
            this.parquetProvider = parquetProvider;
            return this;
        }

        public Builder withBinaryBackend(BinaryBackend binaryBackend) {
            this.binaryBackend = binaryBackend;
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
        private final String file;
        private final String tmpFile;
        private final ParquetWriter<GenericRecord> parquetWriter;
        private final AtomicInteger writeCounter = new AtomicInteger(0);

        private DataWriter(DatasetUri datasetUri, String filename, Schema schema) throws IOException {
            this.file = resolveFilePath(datasetUri, filename);
            this.tmpFile = this.file + ".tmp";
            parquetWriter = provider.getWriter(backend.write(tmpFile), schema);
        }

        /**
         * Write a record to storage as a parquet file.
         * <p>
         * Note: The record could be buffered. {@link #close()} must be called afterwards to ensure persistence of all buffered records.
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
                backend.delete(tmpFile);
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
                    backend.move(tmpFile, file);
                } else {
                    // Delete the file since no records have been written to it
                    backend.delete(tmpFile);
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
    }
}
