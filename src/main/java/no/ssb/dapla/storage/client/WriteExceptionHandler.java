package no.ssb.dapla.storage.client;

import org.apache.avro.generic.GenericRecord;

import java.util.Optional;

public interface WriteExceptionHandler {

    /**
     * Handle a GenericRecord storage exception and optionally return a GenericRecord
     * that will be reattempted to be written. If no GenericRecord is returned
     * and the handler method itself does not throw an exception,
     * the GenericRecord is simply skipped.
     *
     * This handler can be used if you want to silently ignore a specific write
     * exception or if you want to "repair" a GenericRecord.
     *
     * @param e exception
     * @param record failed GenericRecord
     * @return
     */
    Optional<GenericRecord> handleException(Exception e, GenericRecord record);

}
