package no.ssb.dapla.storage.client.backend.gcs;

import no.ssb.dapla.storage.client.backend.gcs.GoogleCloudStorageBackend.Configuration;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;

class GoogleCloudStorageBackendTest {

    @Disabled("This test runs against gcs")
    @Test
    void thatWriteWorksWhenFileAlreadyExists() throws IOException {

        Configuration configuration = new Configuration();
        configuration.setServiceAccountCredentials(Path.of(URI.create("replace me")));

        GoogleCloudStorageBackend backend = new GoogleCloudStorageBackend(configuration);
        backend.write("gs://ssb-rawdata-dev/a-file", "this is the second line".getBytes());
    }

    @Disabled("This test runs against gcs")
    @Test
    void thatWriteWorksWhenFileDoesntExist() throws IOException {
        Configuration configuration = new Configuration();
        configuration.setServiceAccountCredentials(Path.of(URI.create("replace me")));

        GoogleCloudStorageBackend backend = new GoogleCloudStorageBackend(configuration);
        backend.write("gs://ssb-rawdata-dev/doesnt-exist", "write this line".getBytes());
    }
}
