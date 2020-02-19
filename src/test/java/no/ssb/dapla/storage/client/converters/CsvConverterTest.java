package no.ssb.dapla.storage.client.converters;

import io.reactivex.Flowable;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import static no.ssb.dapla.storage.client.StorageClientTest.DIMENSIONAL_SCHEMA;
import static org.assertj.core.api.Assertions.assertThat;

class CsvConverterTest {

    private CsvConverter converter;

    @BeforeEach
    void setUp() {
        converter = new CsvConverter();
    }

    @Test
    void testEmpty() {

        List<GenericRecord> records = converter.read(
                InputStream.nullInputStream(),
                converter.getMediaType(),
                DIMENSIONAL_SCHEMA
        ).toList().blockingGet();

        assertThat(records).isEmpty();

        converter.write(
                Flowable.empty(),
                OutputStream.nullOutputStream(),
                converter.getMediaType(),
                DIMENSIONAL_SCHEMA
        ).blockingAwait();

    }

    @Test
    void testRead() {

        InputStream csvStream = new ByteArrayInputStream((
                "string,int,boolean,float,long,double\r\n" +
                        "foo,123,true,123.123,123,123.123"
        ).getBytes());

        List<GenericRecord> records = converter.read(csvStream, converter.getMediaType(), DIMENSIONAL_SCHEMA).toList()
                .blockingGet();

        assertThat(records).containsExactly(
                new GenericRecordBuilder(DIMENSIONAL_SCHEMA)
                        .set("string", "foo")
                        .set("int", 123)
                        .set("boolean", true)
                        .set("float", 123.123F)
                        .set("long", 123L)
                        .set("double", 123.123D)
                        .build()
        );
    }

    @Test
    void testWrite() {

        GenericData.Record record = new GenericRecordBuilder(DIMENSIONAL_SCHEMA)
                .set("string", "foo")
                .set("int", 123)
                .set("boolean", true)
                .set("float", 123.123F)
                .set("long", 123L)
                .set("double", 123.123D)
                .build();

        ByteArrayOutputStream output = new ByteArrayOutputStream();

        converter.write(
                Flowable.just(record),
                output,
                converter.getMediaType(), DIMENSIONAL_SCHEMA
        ).blockingAwait();

        assertThat(new String(output.toByteArray())).isEqualTo(
                "string,int,boolean,float,long,double\r\n" +
                "foo,123,true,123.123,123,123.123\r\n"
        );
    }

    @Test
    void testSupportsMimeWithParams() {
        assertThat(converter.doesSupport(converter.getMediaType() + ";charset=utf-8)")).isTrue();
    }

    @Test
    void testSupportsMime() {
        assertThat(converter.doesSupport(converter.getMediaType())).isTrue();
    }
}