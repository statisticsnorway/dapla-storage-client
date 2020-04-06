package no.ssb.dapla.storage.client;

import io.reactivex.Flowable;
import io.reactivex.Observable;
import no.ssb.dapla.dataset.uri.DatasetUri;
import no.ssb.dapla.storage.client.StorageClient.StorageClientException;
import no.ssb.dapla.storage.client.backend.FileInfo;
import no.ssb.dapla.storage.client.backend.gcs.GoogleCloudStorageBackend;
import no.ssb.dapla.storage.client.backend.local.LocalBackend;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.io.FileUtils;
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
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StorageClientTest {

    public static final Schema DIMENSIONAL_SCHEMA = Schema.createRecord("root", "...", "no.ssb.dataset", false, List.of(
            new Schema.Field("string", Schema.create(Schema.Type.STRING), "A string", (Object) null),
            new Schema.Field("int", Schema.create(Schema.Type.INT), "An int", (Object) null),
            new Schema.Field("boolean", Schema.create(Schema.Type.BOOLEAN), "A boolean", (Object) null),
            new Schema.Field("float", Schema.create(Schema.Type.FLOAT), "A float", (Object) null),
            new Schema.Field("long", Schema.create(Schema.Type.LONG), "A long", (Object) null),
            new Schema.Field("double", Schema.create(Schema.Type.DOUBLE), "A double", (Object) null)
    ));

    private GenericRecordBuilder recordBuilder;
    private Path testDir;

    @BeforeEach
    void setUp() throws IOException {
        testDir = Files.createTempDirectory("StorageClientTest");
        recordBuilder = new GenericRecordBuilder(DIMENSIONAL_SCHEMA);
    }

    @AfterEach
    void tearDown() throws IOException {
        FileUtils.deleteDirectory(testDir.toFile());
    }

    @Test
    void thatListByLastModifiedWorks() throws IOException, InterruptedException {
        DatasetUri datasetUri = DatasetUri.of(testDir.toUri().toString(), "a-path", "123");

        Files.createDirectories(Path.of(datasetUri.toURI()));
        Files.write(Path.of(URI.create(datasetUri.toString() + "/file-1")), "content-file-1\n".getBytes(), StandardOpenOption.CREATE);
        Thread.sleep(20L);
        Files.write(Path.of(URI.create(datasetUri.toString() + "/file-2")), "content-file-2\n".getBytes(), StandardOpenOption.CREATE);

        StorageClient client = StorageClient.builder().withBinaryBackend(new LocalBackend()).build();
        List<FileInfo> actual = client.listDatasetFilesByLastModified(datasetUri);
        assertThat(actual).hasSize(2);
        assertThat(actual.get(1).getPath()).contains("file-2");
    }

    @Disabled("This test runs against gcs")
    @Test
    void thatListByLastModifiedWorksInGCS() {
        DatasetUri datasetUri = DatasetUri.of("gs://dev-rawdata-store", "datastore/kilde/ske/freg/person/rådata/v123", "1585640088000");
        StorageClient client = StorageClient.builder().withBinaryBackend(new GoogleCloudStorageBackend()).build();
        client.listDatasetFilesByLastModified(datasetUri).forEach(fileInfo -> System.out.println(fileInfo.getPath() + " @ " + fileInfo.getLastModified()));
    }

    @Test
    void thatListByLastModifiedWorksWhenDirectoryIsEmpty() throws IOException {
        DatasetUri datasetUri = DatasetUri.of(testDir.toUri().toString(), "me-a-path", "111");
        Files.createDirectories(Path.of(datasetUri.toURI()));

        StorageClient client = StorageClient.builder().withBinaryBackend(new LocalBackend()).build();
        List<FileInfo> actual = client.listDatasetFilesByLastModified(datasetUri);
        assertThat(actual).isEmpty();
    }

    @Disabled("This test runs against gcs")
    @Test
    void thatListByLastModifiedWorksWhenDirectoryIsEmptyInGCS() {
        DatasetUri datasetUri = DatasetUri.of("gs://dev-rawdata-store", "test", "555");
        StorageClient client = StorageClient.builder().withBinaryBackend(new GoogleCloudStorageBackend()).build();
        assertThat(client.listDatasetFilesByLastModified(datasetUri)).isEmpty();
    }

    @Test
    void thatListByLastModifiedFailsWhenDirectoryDoesntExist() {
        DatasetUri datasetUri = DatasetUri.of(testDir.toUri().toString(), "me-path", "2222");

        StorageClient client = StorageClient.builder().withBinaryBackend(new LocalBackend()).build();
        assertThatThrownBy(() -> client.listDatasetFilesByLastModified(datasetUri))
                .isInstanceOf(StorageClientException.class)
                .hasMessageContaining("Unable to list by last modified in path")
                .hasCauseInstanceOf(NoSuchFileException.class);
    }

    @Disabled("This test runs against gcs")
    @Test
    void thatListByLastModifiedWorksWhenDirectoryDoesntExistGCS() {
        DatasetUri datasetUri = DatasetUri.of("gs://dev-rawdata-store", "non-existent", "321");
        StorageClient client = StorageClient.builder().withBinaryBackend(new GoogleCloudStorageBackend()).build();
        assertThat(client.listDatasetFilesByLastModified(datasetUri)).isEmpty();
    }

    @Disabled("See FIXME in test method")
    @Test
    void testReadWrite() {

//        Flowable<GenericRecord> records = generateRecords(1, 100);
//        client.writeAllData("test", DIMENSIONAL_SCHEMA, records).blockingAwait();

        //FIXME: Don't use StorageClient to validate test result as this is the class we're testing
//        List<GenericRecord> readRecords = client.readData("test", null).toList().blockingGet();

//        assertThat(readRecords)
//                .usingElementComparator(Comparator.comparing(r -> ((Integer) r.get("int"))))
//                .containsExactlyInAnyOrderElementsOf(records.toList().blockingGet());

    }

    @Test
    void thatReadParquetGroupWithProjectionSchemaWorks() throws IOException {
        DatasetUri datasetUri = DatasetUri.of(testDir.toUri().toString(), "a-filepath", "789");
        Files.createDirectories(Path.of(datasetUri.toURI()));

        //TODO: Find a way to create test data without relying on StorageClient
        StorageClient client = StorageClient.builder().withBinaryBackend(new LocalBackend()).build();
        client.writeAllData(datasetUri, "a-filename", DIMENSIONAL_SCHEMA, generateRecords(1, 10)).blockingAwait();

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
        client.readParquetFile(datasetUri, "a-filename", projectionSchema, group -> {
            values.add(group.getValueToString(0, 0));
        });
        assertThat(values).containsExactly("1", "2", "3", "4", "5", "6", "7", "8", "9", "10");
    }

    @Disabled("This test runs against gcs")
    @Test
    void thatReadParquetGroupWithProjectionSchemaWorksInGCS() {
        DatasetUri datasetUri = DatasetUri.of("gs://dev-rawdata-store", "StorageClientTest", "123");

        //TODO: Find a way to create test data without relying on StorageClient
        StorageClient client = StorageClient.builder().withBinaryBackend(new GoogleCloudStorageBackend()).build();
        client.writeAllData(datasetUri, "a-filename.parquet", DIMENSIONAL_SCHEMA, generateRecords(1, 10)).blockingAwait();

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
        StorageClient client = StorageClient.builder().withBinaryBackend(new GoogleCloudStorageBackend()).build();
        FileInfo fileInfo = client.getLastModifiedDatasetFile(datasetUri).get();
        System.out.println(fileInfo.getPath());
    }

    @Disabled("This test runs against gcs")
    @Test
    void thatGetLastModifiedDatasetFileWorksWhenDirectoryDoesntExistInGCS() {
        DatasetUri datasetUri = DatasetUri.of("gs://dev-rawdata-store", "does-not-exist", "1000000");
        StorageClient client = StorageClient.builder().withBinaryBackend(new GoogleCloudStorageBackend()).build();
        assertThat(client.getLastModifiedDatasetFile(datasetUri)).isEmpty();
    }

    @Disabled("This test runs against gcs")
    @Test
    void thatGetLastModifiedDatasetFileWorksWhenDirectoryIsEmptyInGCS() {
        DatasetUri datasetUri = DatasetUri.of("gs://dev-rawdata-store", "test", "555");
        StorageClient client = StorageClient.builder().withBinaryBackend(new GoogleCloudStorageBackend()).build();
        assertThat(client.getLastModifiedDatasetFile(datasetUri)).isEmpty();
    }

    @Test
    void thatReadParquetFileFailsWhenProjectionSchemaIsNotASubsetOfOriginalSchema() throws IOException {
        DatasetUri datasetUri = DatasetUri.of(testDir.toUri().toString(), "some-filepath", "456");
        Files.createDirectories(Path.of(datasetUri.toURI()));

        //TODO: Find a way to create test data without depending on StorageClient
        StorageClient client = StorageClient.builder().withBinaryBackend(new LocalBackend()).build();
        client.writeAllData(datasetUri, "just-a-filename", DIMENSIONAL_SCHEMA, generateRecords(1, 1)).blockingAwait();

        MessageType projectionSchema = MessageTypeParser.parseMessageType(
                "message unknown {\n" +
                        " required int32 doesnt-exist;\n" +
                        "}"
        );

        assertThatThrownBy(() -> client.readParquetFile(datasetUri, "just-a-filename", projectionSchema, SimpleGroup::toString))
                .isInstanceOf(StorageClientException.class)
                .hasMessageContaining("Failed to read parquet file in path")
                .hasCauseInstanceOf(InvalidRecordException.class);
    }

    @Disabled("This test runs against gcs")
    @Test
    void thatReadParquetFileFailsWhenProjectionSchemaIsNotASubsetOfOriginalSchemaInGCS() throws IOException {
        DatasetUri datasetUri = DatasetUri.of("gs://dev-rawdata-store", "StorageClientTest", "4242");

        //TODO: Find a way to create test data without depending on StorageClient
        StorageClient client = StorageClient.builder().withBinaryBackend(new GoogleCloudStorageBackend()).build();
        client.writeAllData(datasetUri, "a-test-file.parquet", DIMENSIONAL_SCHEMA, generateRecords(1, 1)).blockingAwait();

        MessageType projectionSchema = MessageTypeParser.parseMessageType(
                "message unknown {\n" +
                        " required int32 doesnt-exist;\n" +
                        "}"
        );

        assertThatThrownBy(() -> client.readParquetFile(datasetUri, "a-test-file.parquet", projectionSchema, SimpleGroup::toString))
                .isInstanceOf(StorageClientException.class)
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
        Flowable<PositionedRecord> recordFlowable = unlimitedFlowable.map(
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

        StorageClient client = StorageClient.builder().withBinaryBackend(new LocalBackend()).build();
        Observable<PositionedRecord> feedBack = client.writeDataUnbounded(
                datasetUri,
                () -> "test2-testUnbounded" + System.currentTimeMillis() + ".parquet",
                DIMENSIONAL_SCHEMA,
                recordFlowable,
                1,
                TimeUnit.MINUTES,
                10
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
        Flowable<PositionedRecord> recordFlowable = unlimitedFlowable.map(
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

        StorageClient client = StorageClient.builder().withBinaryBackend(new GoogleCloudStorageBackend()).build();
        DatasetUri datasetUri = DatasetUri.of("gs://dev-rawdata-store", "StorageClientTest", "1000000");

        Observable<PositionedRecord> feedBack = client.writeDataUnbounded(
                datasetUri,
                () -> "test2-testUnbounded" + System.currentTimeMillis() + ".parquet",
                DIMENSIONAL_SCHEMA,
                recordFlowable,
                1,
                TimeUnit.MINUTES,
                10
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
