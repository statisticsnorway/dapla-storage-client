package no.ssb.dapla.storage.client;

import com.google.common.base.Strings;
import de.huxhorn.sulky.ulid.ULID;
import io.reactivex.Completable;
import io.reactivex.Flowable;
import io.reactivex.Maybe;
import io.reactivex.Observable;
import no.ssb.dapla.storage.client.backend.BinaryBackend;
import no.ssb.dapla.storage.client.backend.FileInfo;
import no.ssb.dapla.storage.client.converters.CsvConverter;
import no.ssb.dapla.storage.client.converters.FormatConverter;
import no.ssb.dapla.storage.client.converters.JsonConverter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.SeekableByteChannel;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * Data client is an abstraction to read and write Parquet files on bucket storage.
 * <p>
 * The data client supports CSV and JSON type conversions and can be extended by implementing the
 * {@link FormatConverter} interface (see {@link CsvConverter} and
 * {@link JsonConverter} for examples).
 */
public class StorageClient {

    private final BinaryBackend backend;
    private final List<FormatConverter> converters;
    private final ParquetProvider provider;
    private final Configuration configuration;

    private StorageClient(Builder builder) {
        this.backend = Objects.requireNonNull(builder.binaryBackend);
        this.converters = Objects.requireNonNull(builder.converters);
        this.provider = Objects.requireNonNull(builder.parquetProvider);
        this.configuration = Objects.requireNonNull(builder.configuration);
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Checks if the client can convert from and to a media type
     *
     * @param mediaType the media type to check against
     * @return true if the data client has a converter that support the media type.
     */
    public boolean canConvert(String mediaType) {
        for (FormatConverter converter : converters) {
            if (converter.doesSupport(mediaType)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Convert and write binary data.
     *
     * @param dataId    an opaque identifier for the data.
     * @param schema    the schema used to parse the data.
     * @param input     the binary data.
     * @param mediaType the media type of the binary data.
     * @return a completable that completes once the data is saved.
     * @throws UnsupportedMediaTypeException if the client does not support the media type.
     */
    public Completable convertAndWrite(String dataId, Schema schema, InputStream input, String mediaType) throws UnsupportedMediaTypeException {
        for (FormatConverter converter : converters) {
            if (converter.doesSupport(mediaType)) {
                Flowable<GenericRecord> records = converter.read(input, mediaType, schema);
                return writeAllData(dataId, schema, records);
            }
        }
        throw new UnsupportedMediaTypeException("unsupported type " + mediaType);
    }

    /**
     * Read data and convert to binary data.
     *
     * @param dataId       an opaque identifier for the data.
     * @param schema       the schema used to parse the data.
     * @param outputStream the output stream to read the data into.
     * @param mediaType    the media type of the binary data.
     * @return a completable that completes once the data is read.
     * @throws UnsupportedMediaTypeException if the client does not support the media type.
     */
    public Completable readAndConvert(String dataId, Schema schema, OutputStream outputStream, String mediaType) throws UnsupportedMediaTypeException {
        return readAndConvert(dataId, schema, outputStream, mediaType, null);
    }

    public Completable readAndConvert(String dataId, Schema schema, OutputStream outputStream,
                                      String mediaType, Cursor<Long> cursor) throws UnsupportedMediaTypeException {
        for (FormatConverter converter : converters) {
            if (converter.doesSupport(mediaType)) {
                Flowable<GenericRecord> records = readData(dataId, schema, cursor);
                return converter.write(records, outputStream, mediaType, schema);
            }
        }
        throw new UnsupportedMediaTypeException("unsupported type " + mediaType);
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
     * @param schema the schema used to create the records.
     * @param cursor a cursor on record number.
     * @return a {@link Flowable} of records.
     */
    public Flowable<GenericRecord> readData(String dataId, Schema schema, Cursor<Long> cursor) {
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
            return readRecords(dataId, schema, filter).limit(size);
        } else {
            return readRecords(dataId, schema, FilterCompat.NOOP);
        }
    }

    public Maybe<GenericRecord> readLatestRecord(String dataId, Schema schema) {
        try {
            return backend.list(pathTo(dataId), Comparator.comparing(FileInfo::getLastModified))
                    .filter(fileInfo -> !fileInfo.isDirectory() && !Strings.nullToEmpty(fileInfo.getPath()).endsWith(".tmp"))
                    .lastElement()
                    .map(fileInfo -> {
                        ParquetMetadata metadata = readMetadata(fileInfo.getPath());
                        Long size = 0L;
                        for (BlockMetaData block : metadata.getBlocks()) {
                            size += block.getRowCount();
                        }

                  return readData(fileInfo.getPath(), schema, new Cursor<>(1, size)).firstElement().blockingGet();
              });
        } catch (IOException e) {
            throw new RuntimeException("Unable to list dataset path " + pathTo(dataId), e);
        }
    }

    private Flowable<GenericRecord> readRecords(String dataId, Schema schema, FilterCompat.Filter filter) {
        return Flowable.generate(() -> {
            SeekableByteChannel readableChannel = backend.read(pathTo(dataId));
            return provider.getReader(readableChannel, schema, filter);
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

    /** Return the full path to data, including location as defined by {@link no.ssb.dapla.storage.client.StorageClient.Configuration} */
    private String pathTo(String dataId) {
        String rootPath = configuration.getLocation().replaceFirst("/*$", "");
        return dataId.startsWith(rootPath) ? dataId : rootPath + "/" + dataId;
    }

    public ParquetMetadata readMetadata(String dataId) throws IOException {
        try (SeekableByteChannel channel = backend.read(pathTo(dataId))) {
            ParquetFileReader parquetFileReader = provider.getMetadata(channel);
            return parquetFileReader.getFooter();
        }
    }

    public static class Configuration {

        private String location;

        public Configuration() {
        }

        public String getLocation() {
            return location;
        }

        public void setLocation(String location) {
            this.location = location;
        }
    }

    public static class Builder {

        private ParquetProvider parquetProvider;
        private BinaryBackend binaryBackend;
        private List<FormatConverter> converters = new ArrayList<>();
        private Configuration configuration;


        public Builder withParquetProvider(ParquetProvider parquetProvider) {
            this.parquetProvider = parquetProvider;
            return this;
        }

        public Builder withBinaryBackend(BinaryBackend binaryBackend) {
            this.binaryBackend = binaryBackend;
            return this;
        }

        public Builder withFormatConverter(FormatConverter formatConverter) {
            this.converters.add(formatConverter);
            return this;
        }

        public Builder withFormatConverters(List<FormatConverter> formatConverters) {
            this.converters.addAll(formatConverters);
            return this;
        }

        public Builder withConfiguration(Configuration configuration) {
            this.configuration = configuration;
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
}
