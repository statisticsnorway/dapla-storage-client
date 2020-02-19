package no.ssb.dapla.storage.client;

import io.reactivex.Flowable;
import io.reactivex.Observable;
import io.reactivex.Single;
import no.ssb.dapla.storage.client.backend.local.LocalBackend;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class StorageClientTest {

    public static final Schema DIMENSIONAL_SCHEMA = Schema.createRecord("root", "...", "no.ssb.dataset", false, List.of(
            new Schema.Field("string", Schema.create(Schema.Type.STRING), "A string", (Object) null),
            new Schema.Field("int", Schema.create(Schema.Type.INT), "An int", (Object) null),
            new Schema.Field("boolean", Schema.create(Schema.Type.BOOLEAN), "A boolean", (Object) null),
            new Schema.Field("float", Schema.create(Schema.Type.FLOAT), "A float", (Object) null),
            new Schema.Field("long", Schema.create(Schema.Type.LONG), "A long", (Object) null),
            new Schema.Field("double", Schema.create(Schema.Type.DOUBLE), "A double", (Object) null)
    ));

    private StorageClient client;
    private String prefix;
    private GenericRecordBuilder recordBuilder;

    @BeforeEach
    void setUp() throws IOException {

        ParquetProvider.Configuration parquetConfiguration = new ParquetProvider.Configuration();
        parquetConfiguration.setPageSize(128);
        parquetConfiguration.setRowGroupSize(8 * 128);

        StorageClient.Configuration clientConfiguration = new StorageClient.Configuration();

        prefix = Files.createTempDirectory("lds-data-client").toString();
        clientConfiguration.setLocation(prefix);

        client = StorageClient.builder()
                .withParquetProvider(new ParquetProvider(parquetConfiguration))
                .withBinaryBackend(new LocalBackend(prefix))
                .withConfiguration(clientConfiguration)
                .build();
        recordBuilder = new GenericRecordBuilder(DIMENSIONAL_SCHEMA);
    }



    @Test
    void testWithCursor() throws IOException {

        Flowable<GenericRecord> records = generateRecords(1000);
        client.writeAllData("test", DIMENSIONAL_SCHEMA, records).blockingAwait();


        ParquetMetadata metadata = client.readMetadata("test");
        Long size = 0L;
        for (BlockMetaData block : metadata.getBlocks()) {
            size += block.getRowCount();
        }

        Single<GenericRecord> last = client.readData("test", DIMENSIONAL_SCHEMA,
                new Cursor<>(1, size - 1)
        ).firstOrError();

        GenericRecord genericRecord = last.blockingGet();
        assertThat(genericRecord.get("int")).isEqualTo(999);

    }

    private Flowable<GenericRecord> generateRecords(int count) {
        GenericRecordBuilder record = recordBuilder
                .set("string", "foo")
                .set("boolean", true)
                .set("float", 123.123F)
                .set("long", 123L)
                .set("double", 123.123D);

        return Flowable.range(1, count)
                .map(integer -> record.set("int", integer).build());
    }

    @Test
    void testReadWRite() {

        Flowable<GenericRecord> records = generateRecords(100);
        client.writeAllData("test", DIMENSIONAL_SCHEMA, records).blockingAwait();
        List<GenericRecord> readRecords = client.readData("test", DIMENSIONAL_SCHEMA, null)
                .toList().blockingGet();

        assertThat(readRecords)
                .usingElementComparator(Comparator.comparing(r -> ((Integer) r.get("int"))))
                .containsExactlyInAnyOrderElementsOf(records.toList().blockingGet());

    }

    @Test
    void testUnbounded() {

        // Demonstrate the use of windowing (time and size) with the data client.

        AtomicLong counter = new AtomicLong(0);
        Random random = new Random();
        Flowable<Long> unlimitedFlowable = Flowable.generate(emitter -> {

            if (counter.incrementAndGet() <= 500) {
                // System.out.println(Thread.currentThread() + ": emit " + counter.get());
                emitter.onNext(counter.get());
            } else {
                // System.out.println(Thread.currentThread() + ": emit done");
                emitter.onComplete();
            }
            try {
                Thread.sleep(random.nextInt(5) + 5);
            } catch (InterruptedException ie) {
                Thread.interrupted();
            }
        });

        // Convert to record that contains extra information.
        Flowable<PositionedRecord> recordFlowable = unlimitedFlowable.map(income -> {
            GenericData.Record record = recordBuilder.set("string", income.toString()).set("int", income)
                    .set("boolean", income % 2 == 0).set("float", income / 2).set("long", income)
                    .set("double", income / 2).build();
            return new PositionedRecord(record, income / 10);
        });

        Observable<PositionedRecord> feedBack = client.writeDataUnbounded(
                () -> "testUnbounded" + System.currentTimeMillis(),
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