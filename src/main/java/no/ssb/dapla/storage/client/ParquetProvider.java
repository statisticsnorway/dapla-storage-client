package no.ssb.dapla.storage.client;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
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

    private final Configuration configuration;

    public ParquetProvider(Configuration configuration) {
        this.configuration = configuration;
    }

    /**
     * Returns a reader for the file.
     */
    public ParquetFileReader getParquetFileReader(SeekableByteChannel input) throws IOException {
        return ParquetFileReader.open(new SeekableByteChannelInputFile(input));
    }

    public ParquetReader<GenericRecord> getReader(SeekableByteChannel input, Filter filter) throws IOException {
        SeekableByteChannelInputFile inputFile = new SeekableByteChannelInputFile(input);
        return AvroParquetReader.<GenericRecord>builder(inputFile)
                .withFilter(filter)
                .build();
    }

    public ParquetWriter<GenericRecord> getWriter(SeekableByteChannel output, Schema schema) throws IOException {
        ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(new OutputFile() {

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
        }).withSchema(schema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withPageSize(configuration.getPageSize())
                .withRowGroupSize(configuration.getRowGroupSize())
                .build();
        return writer;
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

    public static class Configuration {

        private Integer rowGroupSize;
        private Integer pageSize;

        public Configuration() {
        }

        public Integer getRowGroupSize() {
            return rowGroupSize;
        }

        public void setRowGroupSize(Integer rowGroupSize) {
            this.rowGroupSize = rowGroupSize;
        }

        public Integer getPageSize() {
            return pageSize;
        }

        public void setPageSize(Integer pageSize) {
            this.pageSize = pageSize;
        }

    }

}
