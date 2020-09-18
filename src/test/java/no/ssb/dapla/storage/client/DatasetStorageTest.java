package no.ssb.dapla.storage.client;

import com.google.common.collect.ImmutableSet;
import io.reactivex.Flowable;
import io.reactivex.Observable;
import no.ssb.dapla.dataset.uri.DatasetUri;
import no.ssb.dapla.storage.client.backend.FileInfo;
import no.ssb.dapla.storage.client.backend.gcs.GoogleCloudStorageBackend;
import no.ssb.dapla.storage.client.backend.local.LocalBackend;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.io.InvalidRecordException;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type.Repetition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DatasetStorageTest {

    private static final Schema SCHEMA = new Schema.Parser().parse("""
            {
              "type": "record",
              "name": "root",
              "namespace": "no.ssb.dataset",
              "doc": "...",
              "fields": [
                {
                  "name": "string",
                  "type": "string",
                  "doc": "A string"
                },
                {
                  "name": "int",
                  "type": "int",
                  "doc": "An int"
                },
                {
                  "name": "boolean",
                  "type": "boolean",
                  "doc": "A boolean"
                },
                {
                  "name": "float",
                  "type": "float",
                  "doc": "A float"
                },
                {
                  "name": "long",
                  "type": "long",
                  "doc": "A long"
                },
                {
                  "name": "double",
                  "type": "double",
                  "doc": "A double"
                }
              ]
            }""");

    private GenericRecordBuilder recordBuilder;
    private Path testDir;

    @BeforeEach
    void setUp() throws IOException {
        testDir = Files.createTempDirectory("DatasetStorageTest");
        recordBuilder = new GenericRecordBuilder(SCHEMA);
    }

    @AfterEach
    void tearDown() throws IOException {
        FileUtils.deleteDirectory(testDir.toFile());
    }

    @Test
    void thatReadParquetRecordsWorks() {
        DatasetStorage client = DatasetStorage.builder().withBinaryBackend(new LocalBackend()).build();

        /*
        Dataset made up of two dataset files, each containing 100 records. Schema:
        message root {
          required group person {
            required binary id (UTF8);
          }
          optional group addresses (LIST) {
            repeated group array {
              required binary streetName (UTF8);
            }
          }
        }
        The field streetName contains values ranging from 'Duckburg Lane 0' through 'Duckburg Lane 199'
         */
        DatasetUri uri = DatasetUri.of(Path.of("src/test/resources").toUri().toString(), "data/simple-person", "v1");

        // Read all streetName fields and extract street number from them
        Set<Object> streetNumbers = ImmutableSet.copyOf(
                client.readParquetRecords(uri, Set.of("/addresses/streetName"), (field, value) -> StringUtils.getDigits(value))
                        .map(record -> {
                            assertThat(record).containsOnlyKeys("addresses");
                            return (String) ((List<Map<String, Object>>) record.get("addresses")).get(0).get("streetName");
                        })
                        .blockingIterable()
        );

        assertThat(streetNumbers).hasSize(200);
        assertThat(streetNumbers).contains("0", "199");
    }

    @Test
    void thatFailingRecordsCanBeSkippedWithAnExceptionHandler() {
        /*
        Schema with array that doesn't allow null values
         */
        Schema schema = new Schema.Parser().parse("""
                {
                  "type": "record",
                  "name": "person",
                  "fields": [
                    {
                      "name": "address",
                      "type": [
                        "null",
                        {
                          "type": "array",
                          "items": "string"
                        }
                      ],
                      "default": null
                    }
                  ]
                }""");

        //Trigger exception by attempting to write a record with null value in array
        Flowable<GenericData.Record> records = asFlowable(
                new GenericRecordBuilder(schema).set("address", singletonList("1")).build(),
                new GenericRecordBuilder(schema).set("address", Arrays.asList("2", null, "foo")).build(),
                new GenericRecordBuilder(schema).set("address", singletonList("3")).build()
        );

        // Use an exception handler to ignore and skip failing records
        DatasetStorage client = DatasetStorage.builder()
                .withWriteExceptionHandler((e, record) -> Optional.empty())
                .withBinaryBackend(new LocalBackend())
                .build();

        DatasetUri uri = DatasetUri.of(testDir.toUri().toString(), "just-a-path", "897");

        //Write records
        client.writeDataUnbounded(uri, schema, records, 300, TimeUnit.SECONDS, 1000)
                .subscribe(
                        record -> {
                        },
                        throwable -> {
                        }
                );

        //Read back records
        List<GenericRecord> got = client.readAvroRecords(uri);

        //All the *valid* records should have been written
        assertThat(got).containsExactlyInAnyOrder(
                new GenericRecordBuilder(schema).set("address", singletonList("1")).build(),
                new GenericRecordBuilder(schema).set("address", singletonList("3")).build()
        );
    }

    @Test
    void thatWhenARecordFailsTheRecordsPrecedingItAreKept() {
        /*
        Schema with array that doesn't allow null values
         */
        Schema schema = new Schema.Parser().parse("""
                {
                  "type": "record",
                  "name": "person",
                  "fields": [
                    {
                      "name": "address",
                      "type": [
                        "null",
                        {
                          "type": "array",
                          "items": "string"
                        }
                      ],
                      "default": null
                    }
                  ]
                }""");

        //Trigger exception by attempting to write a record with null value in array
        Flowable<GenericData.Record> records = asFlowable(
                new GenericRecordBuilder(schema).set("address", singletonList("1")).build(),
                new GenericRecordBuilder(schema).set("address", Arrays.asList("2", null, "foo")).build(),
                new GenericRecordBuilder(schema).set("address", singletonList("3")).build()
        );

        DatasetStorage client = DatasetStorage.builder().withBinaryBackend(new LocalBackend()).build();

        DatasetUri uri = DatasetUri.of(testDir.toUri().toString(), "just-a-path", "897");

        //Write records
        client.writeDataUnbounded(uri, schema, records, 300, TimeUnit.SECONDS, 1000)
                .subscribe(
                        record -> {
                        },
                        throwable -> {
                        }
                );

        //Read back records
        List<GenericRecord> got = client.readAvroRecords(uri);

        //The first record should have been written
        assertThat(got).containsExactlyInAnyOrder(new GenericRecordBuilder(schema).set("address", singletonList("1")).build());
    }

    @Test
    void thatConsecutiveInvalidRecordsWork() {
        /*
        Schema with array that doesn't allow null values
         */
        Schema schema = new Schema.Parser().parse("""
                {
                  "type": "record",
                  "name": "person",
                  "fields": [
                    {
                      "name": "address",
                      "type": [
                        "null",
                        {
                          "type": "array",
                          "items": "string"
                        }
                      ],
                      "default": null
                    }
                  ]
                }""");

        //Trigger exception by attempting to write a records with null values in array
        Flowable<GenericData.Record> records = asFlowable(
                new GenericRecordBuilder(schema).set("address", Arrays.asList("1", null, "foo")).build(),
                new GenericRecordBuilder(schema).set("address", Arrays.asList(null, "bar")).build()
        );

        // Use an exception handler to ignore and skip failing records
        DatasetStorage client = DatasetStorage.builder()
                .withWriteExceptionHandler((e, record) -> Optional.empty())
                .withBinaryBackend(new LocalBackend())
                .build();

        DatasetUri uri = DatasetUri.of(testDir.toUri().toString(), "just-a-path", "897");

        //Write records
        client.writeDataUnbounded(uri, schema, records, 300, TimeUnit.SECONDS, 1000)
                .subscribe(
                        record -> {
                        },
                        throwable -> {
                        }
                );

        //Read back records
        List<GenericRecord> got = client.readAvroRecords(uri);

        assertThat(got).isEmpty();
    }

    @Test
    void thatWeAreAbleToSkipRecordsAcrossWindows() {
        /*
        Schema with array that doesn't allow null values
         */
        Schema schema = new Schema.Parser().parse("""
                {
                  "type": "record",
                  "name": "person",
                  "fields": [
                    {
                      "name": "address",
                      "type": [
                        "null",
                        {
                          "type": "array",
                          "items": "string"
                        }
                      ],
                      "default": null
                    }
                  ]
                }""");

        //Trigger exception by attempting to write a records with null values in array
        Flowable<GenericData.Record> records = asFlowable(
                new GenericRecordBuilder(schema).set("address", Arrays.asList("1", null)).build(),
                new GenericRecordBuilder(schema).set("address", singletonList("bar")).build(),
                new GenericRecordBuilder(schema).set("address", Arrays.asList("2", null)).build(),
                new GenericRecordBuilder(schema).set("address", singletonList("abc")).build(),
                new GenericRecordBuilder(schema).set("address", Arrays.asList(null, "def")).build(),
                new GenericRecordBuilder(schema).set("address", singletonList("ghi")).build()
        );

        // Use an exception handler to ignore and skip failing records
        DatasetStorage client = DatasetStorage.builder()
                .withWriteExceptionHandler((e, record) -> Optional.empty())
                .withBinaryBackend(new LocalBackend())
                .build();

        DatasetUri uri = DatasetUri.of(testDir.toUri().toString(), "just-a-path", "4242");

        //Write with a count window of 3 records
        client.writeDataUnbounded(uri, schema, records, 300, TimeUnit.SECONDS, 3)
                .subscribe(
                        record -> {
                        },
                        throwable -> {
                            throwable.printStackTrace();
                        }
                );

        //Read back records
        List<GenericRecord> got = client.readAvroRecords(uri);
        assertThat(got)
                .containsExactlyInAnyOrder(
                        new GenericRecordBuilder(schema).set("address", singletonList("bar")).build(),
                        new GenericRecordBuilder(schema).set("address", singletonList("abc")).build(),
                        new GenericRecordBuilder(schema).set("address", singletonList("ghi")).build()
                );
    }

    private static Flowable<GenericData.Record> asFlowable(GenericData.Record... records) {
        AtomicInteger counter = new AtomicInteger(0);
        return Flowable.generate(emitter -> {
            if (counter.get() < records.length) {
                emitter.onNext(records[counter.getAndIncrement()]);
                try {
                    Thread.sleep(5);
                } catch (InterruptedException ie) {
                    Thread.interrupted();
                }
            } else {
                emitter.onComplete();
            }
        });
    }

    @Test
    void thatListByLastModifiedWorks() throws IOException, InterruptedException {
        DatasetUri datasetUri = DatasetUri.of(testDir.toUri().toString(), "a-path", "123");

        Files.createDirectories(Path.of(datasetUri.toURI()));
        Files.write(Path.of(URI.create(datasetUri.toString() + "/file-1.parquet")), "content-file-1\n".getBytes(), StandardOpenOption.CREATE);
        Thread.sleep(20L);
        Files.write(Path.of(URI.create(datasetUri.toString() + "/file-2.parquet")), "content-file-2\n".getBytes(), StandardOpenOption.CREATE);
        Thread.sleep(20L);
        Files.write(Path.of(URI.create(datasetUri.toString() + "/file-3.parquet.tmp")), "content-file-2\n".getBytes(), StandardOpenOption.CREATE);

        DatasetStorage client = DatasetStorage.builder().withBinaryBackend(new LocalBackend()).build();
        List<FileInfo> actual = client.listDatasetFilesByLastModified(datasetUri);
        assertThat(actual).hasSize(2);
        assertThat(actual.get(1).getName()).isEqualTo("file-2.parquet");
    }

    @Disabled("This test runs against gcs")
    @Test
    void thatListByLastModifiedWorksInGCS() {
        DatasetUri datasetUri = DatasetUri.of("gs://dev-rawdata-store", "datastore/kilde/ske/freg/person/rådata/v123", "1585640088000");
        DatasetStorage client = DatasetStorage.builder().withBinaryBackend(new GoogleCloudStorageBackend()).build();
        client.listDatasetFilesByLastModified(datasetUri).forEach(fileInfo -> System.out.println(fileInfo.getName() + " @ " + fileInfo.getLastModified()));
    }

    @Test
    void thatListByLastModifiedWorksWhenDirectoryIsEmpty() throws IOException {
        DatasetUri datasetUri = DatasetUri.of(testDir.toUri().toString(), "me-a-path", "111");
        Files.createDirectories(Path.of(datasetUri.toURI()));

        DatasetStorage client = DatasetStorage.builder().withBinaryBackend(new LocalBackend()).build();
        List<FileInfo> actual = client.listDatasetFilesByLastModified(datasetUri);
        assertThat(actual).isEmpty();
    }

    @Disabled("This test runs against gcs")
    @Test
    void thatListByLastModifiedWorksWhenDirectoryIsEmptyInGCS() {
        DatasetUri datasetUri = DatasetUri.of("gs://dev-rawdata-store", "test", "555");
        DatasetStorage client = DatasetStorage.builder().withBinaryBackend(new GoogleCloudStorageBackend()).build();
        assertThat(client.listDatasetFilesByLastModified(datasetUri)).isEmpty();
    }

    @Test
    void thatListByLastModifiedFailsWhenDirectoryDoesntExist() {
        DatasetUri datasetUri = DatasetUri.of(testDir.toUri().toString(), "me-path", "2222");

        DatasetStorage client = DatasetStorage.builder().withBinaryBackend(new LocalBackend()).build();
        assertThat(client.listDatasetFilesByLastModified(datasetUri)).isEmpty();
    }

    @Disabled("This test runs against gcs")
    @Test
    void thatListByLastModifiedWorksWhenDirectoryDoesntExistGCS() {
        DatasetUri datasetUri = DatasetUri.of("gs://dev-rawdata-store", "non-existent", "321");
        DatasetStorage client = DatasetStorage.builder().withBinaryBackend(new GoogleCloudStorageBackend()).build();
        assertThat(client.listDatasetFilesByLastModified(datasetUri)).isEmpty();
    }

    @Test
    void thatReadParquetGroupWithProjectionSchemaWorks() {
        DatasetUri datasetUri = DatasetUri.of(testDir.toUri().toString(), "a-filepath", "789");

        //TODO: Find a way to create test data without relying on StorageClient
        DatasetStorage client = DatasetStorage.builder().withBinaryBackend(new LocalBackend()).build();
        client.writeAllData(datasetUri, SCHEMA, generateRecords(1, 10)).blockingAwait();

        String file = client.listDatasetFilesByLastModified(datasetUri).get(0).getName();

        /*
          message no.ssb.dataset.root {
            required int32 int;
          }
         */
        MessageType projectionSchema = new MessageType(
                "no.ssb.dataset.root",
                new PrimitiveType(
                        Repetition.REQUIRED,
                        PrimitiveTypeName.INT32,
                        "int"
                )
        );

        List<String> values = new ArrayList<>();
        client.readParquetFile(datasetUri, file, projectionSchema, group -> {
            values.add(group.getValueToString(0, 0));
        });
        assertThat(values).containsExactly("1", "2", "3", "4", "5", "6", "7", "8", "9", "10");
    }

    @Disabled("This test runs against gcs")
    @Test
    void thatReadParquetGroupWithProjectionSchemaWorksInGCS() {
        DatasetUri datasetUri = DatasetUri.of("gs://dev-rawdata-store", "StorageClientTest", "123");

        //TODO: Find a way to create test data without relying on StorageClient
        DatasetStorage client = DatasetStorage.builder().withBinaryBackend(new GoogleCloudStorageBackend()).build();
        client.writeAllData(datasetUri, SCHEMA, generateRecords(1, 10)).blockingAwait();

        /*
          message no.ssb.dataset.root {
            required int32 int;
          }
         */
        MessageType projectionSchema = new MessageType(
                "no.ssb.dataset.root",
                new PrimitiveType(
                        Repetition.REQUIRED,
                        PrimitiveTypeName.INT32,
                        "int"
                )
        );

        List<String> values = new ArrayList<>();
        client.readParquetFile(datasetUri, "a-filename.parquet", projectionSchema, group -> {
            values.add(group.getValueToString(0, 0));
        });
        assertThat(values).containsExactly("1", "2", "3", "4", "5", "6", "7", "8", "9", "10");
    }

    @Disabled("This test runs against gcs")
    @Test
    void thatGetLastModifiedDatasetFileWorksInGCS() {
        DatasetUri datasetUri = DatasetUri.of("gs://dev-rawdata-store", "StorageClientTest", "1000000");
        DatasetStorage client = DatasetStorage.builder().withBinaryBackend(new GoogleCloudStorageBackend()).build();
        FileInfo fileInfo = client.getLastModifiedDatasetFile(datasetUri).get();
        System.out.println(fileInfo.getPath());
    }

    @Disabled("This test runs against gcs")
    @Test
    void thatGetLastModifiedDatasetFileWorksWhenDirectoryDoesntExistInGCS() {
        DatasetUri datasetUri = DatasetUri.of("gs://dev-rawdata-store", "does-not-exist", "1000000");
        DatasetStorage client = DatasetStorage.builder().withBinaryBackend(new GoogleCloudStorageBackend()).build();
        assertThat(client.getLastModifiedDatasetFile(datasetUri)).isEmpty();
    }

    @Disabled("This test runs against gcs")
    @Test
    void thatGetLastModifiedDatasetFileWorksWhenDirectoryIsEmptyInGCS() {
        DatasetUri datasetUri = DatasetUri.of("gs://dev-rawdata-store", "test", "555");
        DatasetStorage client = DatasetStorage.builder().withBinaryBackend(new GoogleCloudStorageBackend()).build();
        assertThat(client.getLastModifiedDatasetFile(datasetUri)).isEmpty();
    }

    @Test
    void thatReadParquetFileFailsWhenProjectionSchemaIsNotASubsetOfOriginalSchema() throws IOException {
        DatasetUri datasetUri = DatasetUri.of(testDir.toUri().toString(), "some-filepath", "456");
        Files.createDirectories(Path.of(datasetUri.toURI()));
        Files.createDirectory(Path.of(URI.create(datasetUri.getParentUri() + "/tmp")));

        //TODO: Find a way to create test data without depending on StorageClient
        DatasetStorage client = DatasetStorage.builder().withBinaryBackend(new LocalBackend()).build();
        client.writeAllData(datasetUri, SCHEMA, generateRecords(1, 1)).blockingAwait();

        String file = client.listDatasetFilesByLastModified(datasetUri).get(0).getName();

        MessageType projectionSchema = MessageTypeParser.parseMessageType(
                "message unknown {\n" +
                        " required int32 doesnt-exist;\n" +
                        "}"
        );

        assertThatThrownBy(() -> client.readParquetFile(datasetUri, file, projectionSchema, SimpleGroup::toString))
                .isInstanceOf(DatasetStorageException.class)
                .hasMessageContaining("Failed to read parquet file in path")
                .hasCauseInstanceOf(InvalidRecordException.class);
    }

    @Disabled("This test runs against gcs")
    @Test
    void thatReadParquetFileFailsWhenProjectionSchemaIsNotASubsetOfOriginalSchemaInGCS() throws IOException {
        DatasetUri datasetUri = DatasetUri.of("gs://dev-rawdata-store", "StorageClientTest", "4242");

        //TODO: Find a way to create test data without depending on StorageClient
        DatasetStorage client = DatasetStorage.builder().withBinaryBackend(new GoogleCloudStorageBackend()).build();
        client.writeAllData(datasetUri, SCHEMA, generateRecords(1, 1)).blockingAwait();

        MessageType projectionSchema = MessageTypeParser.parseMessageType(
                "message unknown {\n" +
                        " required int32 doesnt-exist;\n" +
                        "}"
        );

        assertThatThrownBy(() -> client.readParquetFile(datasetUri, "a-test-file.parquet", projectionSchema, SimpleGroup::toString))
                .isInstanceOf(DatasetStorageException.class)
                .hasMessageContaining("Failed to read parquet file in path")
                .hasCauseInstanceOf(InvalidRecordException.class);
    }

    @Test
    void testUnbounded() throws IOException {
        // Demonstrate the use of windowing (time and size) with the data client.

        AtomicLong counter = new AtomicLong(0);
        Random random = new Random();
        Flowable<Long> unlimitedFlowable = Flowable.generate(emitter -> {

            if (counter.incrementAndGet() <= 500) {
                emitter.onNext(counter.get());
            } else {
                emitter.onComplete();
            }
            try {
                Thread.sleep(random.nextInt(5) + 5);
            } catch (InterruptedException ie) {
                Thread.interrupted();
            }
        });

        // Convert to record that contains extra information.
        Flowable<PositionedRecord> records = unlimitedFlowable.map(
                income -> {
                    GenericData.Record record = recordBuilder
                            .set("string", income.toString())
                            .set("int", income)
                            .set("boolean", income % 2 == 0)
                            .set("float", income / 2).set("long", income)
                            .set("double", income / 2)
                            .build();
                    return new PositionedRecord(record, income / 10);
                }
        );

        DatasetUri datasetUri = DatasetUri.of(testDir.toUri().toString(), "whatever-filepath", "888");
        Files.createDirectories(Path.of(datasetUri.toURI()));
        Files.createDirectory(Path.of(URI.create(datasetUri.getParentUri() + "/tmp")));

        DatasetStorage client = DatasetStorage.builder().withBinaryBackend(new LocalBackend()).build();
        Observable<PositionedRecord> feedBack = client.writeDataUnbounded(
                datasetUri, SCHEMA, records, 1, TimeUnit.MINUTES, 10
        );

        List<Long> positions = feedBack.map(positionedRecord -> positionedRecord.getPosition()).toList().blockingGet();

        assertThat(positions).containsExactlyElementsOf(
                Stream.iterate(1L, t -> t + 1).limit(50).collect(Collectors.toList())
        );
    }

    @Disabled("This test runs against gcs")
    @Test
    void testUnboundedInGCS() {
        // Demonstrate the use of windowing (time and size) with the data client.

        AtomicLong counter = new AtomicLong(0);
        Random random = new Random();
        Flowable<Long> unlimitedFlowable = Flowable.generate(emitter -> {

            if (counter.incrementAndGet() <= 500) {
                emitter.onNext(counter.get());
            } else {
                emitter.onComplete();
            }
            try {
                Thread.sleep(random.nextInt(5) + 5);
            } catch (InterruptedException ie) {
                Thread.interrupted();
            }
        });

        // Convert to record that contains extra information.
        Flowable<PositionedRecord> records = unlimitedFlowable.map(
                income -> {
                    GenericData.Record record = recordBuilder
                            .set("string", income.toString())
                            .set("int", income)
                            .set("boolean", income % 2 == 0)
                            .set("float", income / 2).set("long", income)
                            .set("double", income / 2)
                            .build();
                    return new PositionedRecord(record, income / 10);
                }
        );

        DatasetStorage client = DatasetStorage.builder().withBinaryBackend(new GoogleCloudStorageBackend()).build();
        DatasetUri datasetUri = DatasetUri.of("gs://dev-rawdata-store", "StorageClientTest", "1000000");

        Observable<PositionedRecord> feedBack = client.writeDataUnbounded(
                datasetUri, SCHEMA, records, 1, TimeUnit.MINUTES, 10
        );

        List<Long> positions = feedBack.map(positionedRecord -> positionedRecord.getPosition()).toList().blockingGet();

        assertThat(positions).containsExactlyElementsOf(
                Stream.iterate(1L, t -> t + 1).limit(50).collect(Collectors.toList())
        );
    }

    private Flowable<GenericRecord> generateRecords(int offset, int count) {
        GenericRecordBuilder record = recordBuilder
                .set("string", "foo")
                .set("boolean", true)
                .set("float", 123.123F)
                .set("long", 123L)
                .set("double", 123.123D);

        return Flowable.range(offset, count).map(integer -> record.set("int", integer).build());
    }

    private static class PositionedRecord implements GenericRecord {
        private final Long position;
        private final GenericRecord delegate;

        private PositionedRecord(GenericRecord delegate, Long position) {
            this.position = position;
            this.delegate = delegate;
        }

        @Override
        public String toString() {
            return delegate.toString();
        }

        public Long getPosition() {
            return position;
        }

        @Override
        public void put(String s, Object o) {
            delegate.put(s, o);
        }

        @Override
        public Object get(String s) {
            return delegate.get(s);
        }

        @Override
        public void put(int i, Object o) {
            delegate.put(i, o);
        }

        @Override
        public Object get(int i) {
            return delegate.get(i);
        }

        @Override
        public Schema getSchema() {
            return delegate.getSchema();
        }
    }
}
