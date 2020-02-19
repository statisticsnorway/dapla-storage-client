package no.ssb.dapla.storage.client.converters;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.reactivex.Completable;
import io.reactivex.Flowable;
import io.reactivex.schedulers.Schedulers;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

public class JsonConverter implements FormatConverter {

    public static final int BUFFER_SIZE = 1024;
    private static final String MEDIA_TYPE = "application/json";
    private final ObjectMapper mapper;
    private final int bufferSize;

    public JsonConverter(ObjectMapper mapper, int bufferSize) {
        if (bufferSize < BUFFER_SIZE) {
            throw new IllegalArgumentException("buffer cannot be lower than " + BUFFER_SIZE);
        }
        this.mapper = Objects.requireNonNull(mapper);
        this.bufferSize = bufferSize;
    }

    public JsonConverter(ObjectMapper mapper) {
        this(mapper, BUFFER_SIZE);
    }

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

    private Function<JsonNode, Object> getConverter(Schema.Field field) {
        Schema.Type type = field.schema().getType();
        field.schema().getLogicalType();
        Function<JsonNode, Object> converter;
        switch (type) {
            case STRING:
                converter = JsonNode::asText;
                break;
            case INT:
                converter = node -> (int) node.asLong();
                break;
            case LONG:
                converter = JsonNode::asLong;
                break;
            case BOOLEAN:
                converter = JsonNode::asBoolean;
                break;
            case FLOAT:
                converter = n -> (float) n.asDouble();
                break;
            case DOUBLE:
                converter = JsonNode::asDouble;
                break;
            default:
                throw new IllegalArgumentException("Unsupported type");
        }
        return converter;
    }

    @Override
    public Flowable<GenericRecord> read(InputStream input, String mimeType, Schema schema) {

        // Flow of objects.
        Flowable<ObjectNode> nodes = Flowable.generate(() -> {
            JsonParser parser = mapper.getFactory().createParser(input);
            parser.nextToken();
            parser.nextValue();
            return mapper.readValues(parser, ObjectNode.class);
        }, (it, emitter) -> {
            try {
                if (it.hasNext()) {
                    emitter.onNext(it.next());
                } else {
                    emitter.onComplete();
                }
            } catch (Exception e) {
                emitter.onError(e);
            }
        }, it -> it.close());

        //List<Deque<JsonNode>> buffers = new ArrayList<>(fields.size());
        //for (Schema.Field field : fields) {
        //    buffers.add(field.pos(), new ArrayDeque<>(BUFFER_SIZE));
        //}

        //List<Deque<Object>> convertedBuffers = new ArrayList<>(fields.size());
        //for (Schema.Field field : fields) {
        //    convertedBuffers.add(field.pos(), new ArrayDeque<>(BUFFER_SIZE));
        //}

        List<Schema.Field> fields = schema.getFields();
        ArrayList<Function<JsonNode, Object>> converters = new ArrayList<>(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            converters.add(i, getConverter(fields.get(i)));
        }

        // Convert to columns
        Flowable<List<List<JsonNode>>> columns = nodes.buffer(bufferSize).map(rows -> {
            List<List<JsonNode>> group = new ArrayList<>();
            for (Schema.Field field : fields) {
                List<JsonNode> column = new ArrayList<>(bufferSize);
                for (JsonNode row : rows) {
                    column.add(row.get(field.name()));
                }
                group.add(column);
            }
            return group;
        });

        Flowable<List<List<Object>>> singleFlowable = columns.map(cols ->
                Flowable.fromIterable(cols)
                        .zipWith(converters, (column, converter) ->
                                Flowable.fromIterable(column)
                                        .subscribeOn(Schedulers.computation())
                                        .map(converter::apply)
                                        .toList().blockingGet()
                        ).toList().blockingGet()
        );

        return singleFlowable.concatMapIterable(lists -> {
            ArrayList<GenericRecordBuilder> builders = new ArrayList<>(bufferSize);
            for (int i = 0; i < lists.get(0).size(); i++) {
                builders.add(new GenericRecordBuilder(schema));
            }
            for (Schema.Field field : fields) {
                List<Object> column = lists.get(field.pos());
                for (int i = 0; i < column.size(); i++) {
                    builders.get(i).set(field, column.get(i));
                }
            }
            return builders;
        }).map(GenericRecordBuilder::build);
    }

    @Override
    public Completable write(Flowable<GenericRecord> records, OutputStream output, String mimeType, Schema schema) {
        return Completable.using(
                () -> {
                    JsonGenerator generator = mapper.getFactory().createGenerator(output);
                    generator.writeStartArray();
                    return generator;
                },
                generator -> {
                    return records.doOnNext(record -> {
                        generator.writeRawValue(record.toString());
                    }).ignoreElements();
                },
                generator -> {
                    generator.writeEndArray();
                    generator.flush();
                }
        );
    }
}
