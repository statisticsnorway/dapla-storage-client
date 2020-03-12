package no.ssb.dapla.storage.client;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.DelegatingPositionOutputStream;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.SeekableInputStream;

import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.util.Objects;

import static org.apache.parquet.filter2.compat.FilterCompat.Filter;

public class ParquetProvider {

    private Integer rowGroupSize;
    private Integer pageSize;

    public ParquetProvider(Integer rowGroupSize, Integer pageSize) {
        this.rowGroupSize = rowGroupSize;
        this.pageSize = pageSize;
    }

    /**
     * Returns a reader for the file.
     */
    public ParquetFileReader getParquetFileReader(SeekableByteChannel input) throws IOException {
        return ParquetFileReader.open(new SeekableByteChannelInputFile(input));
    }

    public ParquetReader<Group> getParquetGroupReader(SeekableByteChannel input, String readSchema) throws IOException {
        SeekableByteChannelInputFile inputFile = new SeekableByteChannelInputFile(input);

        Configuration conf = new Configuration();
        conf.set(ReadSupport.PARQUET_READ_SCHEMA, readSchema);

        return new ParquetGroupReaderBuilder(inputFile, new GroupReadSupport())
                .withConf(conf)
                .build();
    }

    public ParquetReader<GenericRecord> getReader(SeekableByteChannel input, Filter filter) throws IOException {
        SeekableByteChannelInputFile inputFile = new SeekableByteChannelInputFile(input);
        return AvroParquetReader.<GenericRecord>builder(inputFile)
                .withFilter(filter)
                .build();
    }

    public ParquetWriter<GenericRecord> getWriter(SeekableByteChannel output, Schema schema) throws IOException {
        SeekableByteChannelOutputFile outputFile = new SeekableByteChannelOutputFile(output);
        return AvroParquetWriter.<GenericRecord>builder(outputFile).withSchema(schema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withPageSize(pageSize)
                .withRowGroupSize(rowGroupSize)
                .build();
    }

    private static class ParquetGroupReaderBuilder extends ParquetReader.Builder<Group> {

        private final ReadSupport<Group> readSupport;

        protected ParquetGroupReaderBuilder(InputFile file, ReadSupport<Group> readSupport) {
            super(file);
            this.readSupport = readSupport;
        }

        @Override
        protected ReadSupport<Group> getReadSupport() {
            return readSupport;
        }
    }

    private static class SeekableByteChannelOutputFile implements OutputFile {

        private final SeekableByteChannel output;

        private SeekableByteChannelOutputFile(SeekableByteChannel output) {
            this.output = Objects.requireNonNull(output);
        }

        @Override
        public PositionOutputStream create(long blockSizeHint) {
            return new DelegatingPositionOutputStream(Channels.newOutputStream(output)) {
                @Override
                public long getPos() throws IOException {
                    return output.position();
                }
            };
        }

        @Override
        public PositionOutputStream createOrOverwrite(long blockSizeHint) {
            return new DelegatingPositionOutputStream(Channels.newOutputStream(output)) {
                @Override
                public long getPos() throws IOException {
                    return output.position();
                }
            };
        }

        @Override
        public boolean supportsBlockSize() {
            return false;
        }

        @Override
        public long defaultBlockSize() {
            return 0;
        }
    }

    private static class SeekableByteChannelInputFile implements InputFile {

        private final SeekableByteChannel input;

        private SeekableByteChannelInputFile(SeekableByteChannel input) {
            this.input = Objects.requireNonNull(input);
        }

        @Override
        public long getLength() throws IOException {
            return input.size();
        }

        @Override
        public SeekableInputStream newStream() {
            return new DelegatingSeekableInputStream(Channels.newInputStream(input)) {
                @Override
                public long getPos() throws IOException {
                    return input.position();
                }

                @Override
                public void seek(long newPos) throws IOException {
                    input.position(newPos);
                }
            };
        }
    }
}
