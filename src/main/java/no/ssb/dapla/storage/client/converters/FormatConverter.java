package no.ssb.dapla.storage.client.converters;

import io.reactivex.Completable;
import io.reactivex.Flowable;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.io.InputStream;
import java.io.OutputStream;

public interface FormatConverter {

    /**
     * Returns true if this converter can handle the media type.
     */
    boolean doesSupport(String mediaType);

    String getMediaType();

    /**
     * Convert the input stream to {@link GenericRecord}s.
     */
    Flowable<GenericRecord> read(InputStream input, String mimeType, Schema schema);

    /**
     * Convert the {@link Flowable<GenericRecord>} to the output stream.
     */
    Completable write(Flowable<GenericRecord> records, OutputStream output, String mimeType, Schema schema);
}
