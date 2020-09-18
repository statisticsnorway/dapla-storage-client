package no.ssb.dapla.storage.client;

import io.reactivex.Completable;
import io.reactivex.Flowable;
import io.reactivex.Observable;
import no.ssb.dapla.dataset.uri.DatasetUri;
import no.ssb.dapla.parquet.FieldInterceptor;
import no.ssb.dapla.parquet.RecordStream;
import no.ssb.dapla.storage.client.backend.BinaryBackend;
import no.ssb.dapla.storage.client.backend.FileInfo;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * DatasetStorageClient provides methods for reading and writing datasets
 */
public class DatasetStorage {
    private static final Logger log = LoggerFactory.getLogger(DatasetStorage.class);
    private final BinaryBackend backend;
    private final ParquetProvider provider;
    private final WriteExceptionHandler writeExceptionHandler;

    private DatasetStorage(Builder builder) {
        this.backend = Objects.requireNonNull(builder.binaryBackend);
        this.provider = Objects.requireNonNull(builder.parquetProvider);
        this.writeExceptionHandler = builder.writeExceptionHandler;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Read all avro records in a dataset.
     *
     * @param datasetUri the dataset to read
     * @return the records as a list of {@link GenericRecord}
     */
    public List<GenericRecord> readAvroRecords(DatasetUri datasetUri) {
        List<GenericRecord> records = new ArrayList<>();
        for (FileInfo file : listDatasetFiles(datasetUri)) {

            SeekableByteChannel input;
            try {
                input = backend.read(file.getPath());
            } catch (IOException e) {
                throw new DatasetStorageException(String.format("Failed to read dataset file: \"%s\"", file.getPath()), e);
            }

            try (ParquetReader<GenericRecord> reader = provider.getAvroParquetReader(input)) {
                GenericRecord record;
                while ((record = reader.read()) != null) {
                    records.add(record);
                }
            } catch (IOException e) {
                throw new DatasetStorageException(String.format("Failed to read records from file: \"%s\"", file.getPath()), e);
            }
        }
        return records;
    }

    /**
     * Write an unbounded sequence of {@link GenericRecord}s to a given dataset.
     * <p>
     * The records will be written in "batches" of size count or when the timespan duration elapsed. The last value of
     * each batch is returned in an {@link Observable}.
     * <p>
     *
     * @param datasetUri  the dataset to write to.
     * @param records     the records to write.
     * @param timeWindow  the period of time before a batch should be written.
     * @param unit        the unit of time that applies to the timespan argument.
     * @param countWindow the maximum size of a batch before it should be written.
     * @return an {@link Observable} emitting the last record in each batch.
     */
    public <R extends GenericRecord> Observable<R> writeDataUnbounded(
            DatasetUri datasetUri, Schema schema, Flowable<R> records, long timeWindow, TimeUnit unit, long countWindow
    ) {
        return records
                .window(timeWindow, unit, countWindow, true)
                .switchMapMaybe(
                        recordsWindow -> writeData(datasetUri, schema, recordsWindow).lastElement()
                )
                .toObservable();
    }

    /**
     * Write a sequence of {@link GenericRecord}s to a given dataset.
     * <p>
     *
     * @param datasetUri the dataset to write to.
     * @param schema     the schema used to create the records.
     * @param records    the records to write.
     * @return a completable that completes once the data is saved.
     */
    public Completable writeAllData(DatasetUri datasetUri, Schema schema, Flowable<GenericRecord> records) {
        return writeData(datasetUri, schema, records).ignoreElements();
    }

    /**
     * Write a sequence of {@link GenericRecord}s to a given dataset.
     * <p>
     *
     * @param datasetUri the dataset to write to.
     * @param schema     the schema used to create the records.
     * @param records    the records to write.
     * @return a completable that completes once the data is saved.
     */
    public <R extends GenericRecord> Flowable<R> writeData(DatasetUri datasetUri, Schema schema, Flowable<R> records) {
        return Flowable.defer(() -> {
            DataWriter writer = new DataWriter(datasetUri, schema);
            return records
                    .doAfterNext(writer::write)
                    .doOnComplete(writer::close)
                    .doOnError(throwable -> writer.close());
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
            throw new DatasetStorageException(String.format("Unable to list by last modified in path '%s'", datasetUri.toString()), e);
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
            throw new DatasetStorageException(String.format("Failed to read parquet file in path '%s'", filePath), e);
        }
    }

    public Flowable<Map<String, Object>> readParquetRecords(DatasetUri datasetUri, Set<String> fieldSelectors, FieldInterceptor fieldInterceptor) {
        // Find dataset files
        List<FileInfo> files = listDatasetFiles(datasetUri);
        if (files.isEmpty()) {
            return Flowable.empty();
        }

        // Create a record stream for each dataset file and merge them into one
        Flowable<Map<String, Object>> previous = null;
        for (FileInfo file : files) {
            Flowable<Map<String, Object>> records = readParquetRecords(file.getPath(), fieldSelectors, fieldInterceptor);
            if (previous == null) {
                previous = records;
                continue;
            }
            previous = previous.mergeWith(records);
        }
        return previous;
    }

    public Flowable<Map<String, Object>> readParquetRecords(String filePath, Set<String> fieldSelectors, FieldInterceptor fieldInterceptor) {
        return Flowable.generate(
                () -> {
                    SeekableByteChannel channel = backend.read(filePath);
                    return RecordStream.builder(channel).withFieldSelectors(fieldSelectors).withFieldInterceptor(fieldInterceptor).build();
                }, (stream, emitter) -> {
                    Map<String, Object> record;
                    try {
                        record = stream.read();
                    } catch (Exception e) {
                        emitter.onError(e);
                        emitter.onComplete();
                        stream.close();
                        return;
                    }
                    if (record == null) {
                        emitter.onComplete();
                        stream.close();
                        return;
                    }
                    emitter.onNext(record);
                },
                RecordStream::close
        );
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
        private WriteExceptionHandler writeExceptionHandler;

        public Builder withParquetProvider(ParquetProvider parquetProvider) {
            this.parquetProvider = parquetProvider;
            return this;
        }

        public Builder withBinaryBackend(BinaryBackend binaryBackend) {
            this.binaryBackend = binaryBackend;
            return this;
        }

        public Builder withWriteExceptionHandler(WriteExceptionHandler writeExceptionHandler) {
            this.writeExceptionHandler = writeExceptionHandler;
            return this;
        }

        public DatasetStorage build() {
            return new DatasetStorage(this);
        }
    }

    /**
     * Write avro records to parquet files
     */
    class DataWriter implements AutoCloseable {

        private final DatasetUri datasetUri;
        private final Schema schema;

        // Full path - including filename - to the parquet file that will hold the records
        private String dataFile;

        // Full path - including filename - to a temporary file used to buffer records
        private String tmpFile;
        private ParquetWriter<GenericRecord> parquetWriter;
        private AtomicInteger writeCounter;

        private DataWriter(DatasetUri datasetUri, Schema schema) throws IOException {
            this.datasetUri = datasetUri;
            this.schema = schema;
            initWriter();
        }

        private void initWriter() throws IOException {
            String fileName = String.format("%d", System.currentTimeMillis());
            this.tmpFile = datasetUri.getParentUri() + "/tmp/" + fileName;

            String pathToDataFile = datasetUri.toString();
            if (!pathToDataFile.endsWith("/")) {
                pathToDataFile = pathToDataFile + "/";
            }
            this.dataFile = pathToDataFile + fileName + ".parquet";
            this.writeCounter = new AtomicInteger(0);

            this.parquetWriter = provider.getWriter(backend.write(this.tmpFile), this.schema);
        }

        /**
         * Write a record to a parquet file.
         * <p>
         * Note: The record could be buffered. {@link #close()} must be called afterwards to ensure persistence of all buffered records.
         *
         * @param record the record to write.
         */
        public void write(GenericRecord record) {
            try {
                this.writeParquet(record);
                writeCounter.incrementAndGet();
            } catch (Exception e) {

                try {
                    close();
                } catch (IOException ioException) {
                    throw new RuntimeException("Unable to close DataWriter", ioException);
                }

                try {
                    initWriter();
                } catch (IOException ioException) {
                    throw new RuntimeException("Failed to reinitialize writer", ioException);
                }

                if (writeExceptionHandler != null) {
                    writeExceptionHandler.handleException(e, record).ifPresent(this::writeParquet);
                } else {
                    log.error("Failed to write GenericRecord:\n{}", record.toString());
                    throw new DatasetStorageException("Error writing GenericRecord", e);
                }
            }
        }

        private void writeParquet(GenericRecord record) {
            try {
                parquetWriter.write(record);
            } catch (IOException e) {
                throw new DatasetStorageException(e);
            }
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
                    // Temp file contains at least one record, rename it to make it accessible
                    backend.move(tmpFile, dataFile);
                } else {
                    // Temp file is empty, delete it
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
}
