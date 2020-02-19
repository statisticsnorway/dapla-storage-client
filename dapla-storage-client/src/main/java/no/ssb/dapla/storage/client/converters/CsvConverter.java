package no.ssb.dapla.storage.client.converters;

import io.reactivex.Completable;
import io.reactivex.Flowable;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

import static io.reactivex.Flowable.fromIterable;

public class CsvConverter implements FormatConverter {

    private static final String MEDIA_TYPE = "text/csv";

    @Override
    public boolean doesSupport(String mediaType) {
        if (mediaType == null) {
            return false;
        }
        return mediaType.startsWith(MEDIA_TYPE);
    }

    @Override
    public String getMediaType() {
        return MEDIA_TYPE;
    }

    @Override
    public Flowable<GenericRecord> read(InputStream input, String mimeType, Schema schema) {
        // TODO: Detect charset
        // TODO: Support Hierarchical dataset.
        // TODO: Fail if no headers.
        return Flowable.defer(() -> {
            CSVParser records = CSVFormat.RFC4180.withFirstRecordAsHeader().parse(new InputStreamReader(input));
            return fromIterable(records).doFinally(records::close).map(record -> encodeRecord(record, schema));
        });
    }

    @Override
    public Completable write(Flowable<GenericRecord> records, OutputStream output, String mimeType, Schema schema) {
        // TODO: Detect charset
        // TODO: Support Hierarchical dataset.
        // TODO: Fail if no headers.
        List<String> names = new ArrayList<>();
        for (Schema.Field field : schema.getFields()) {
            names.add(field.name());
        }
        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(output));
        return Completable.defer(() -> {
            try {
                CSVPrinter csvPrinter = CSVFormat.RFC4180.withHeader(names.toArray(new String[]{}))
                        .print(out);
                return records.doOnNext(genericRecord -> {
                    List<Object> values = new ArrayList<>();
                    for (String name : names) {
                        values.add(genericRecord.get(name));
                    }
                    csvPrinter.printRecord(values);
                }).doFinally(() -> out.close()).ignoreElements();
            } catch (IOException e) {
                return Completable.error(e);
            }
        });
    }

    private GenericRecord encodeRecord(CSVRecord record, Schema schema) {
        GenericRecordBuilder recordBuilder = new GenericRecordBuilder(schema);
        for (Schema.Field field : schema.getFields()) {
            String name = field.name();
            Schema.Type type = field.schema().getType();
            String value = record.get(name);
            switch (type) {
                case STRING:
                    recordBuilder.set(name, value);
                    break;
                case INT:
                    recordBuilder.set(name, Integer.parseInt(value));
                    break;
                case LONG:
                    recordBuilder.set(name, Long.parseLong(value));
                    break;
                case BOOLEAN:
                    recordBuilder.set(name, Boolean.parseBoolean(value));
                    break;
                case FLOAT:
                    recordBuilder.set(name, Float.parseFloat(value));
                    break;
                case DOUBLE:
                    recordBuilder.set(name, Double.parseDouble(value));
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported type");
            }
        }
        return recordBuilder.build();
    }
}
