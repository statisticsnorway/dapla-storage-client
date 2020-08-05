package no.ssb.dapla.storage.client.backend.gcs;

import com.github.davidmoten.rx2.Bytes;
import io.reactivex.Completable;
import no.ssb.dapla.storage.client.backend.gcs.GoogleCloudStorageBackend.Configuration;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

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

    @Disabled("This test runs against gcs")
    @Test
    void thatWriteWorksWithInputStream() throws IOException {
        Configuration configuration = new Configuration();
        configuration.setServiceAccountCredentials(Path.of("gcs-secret/gcs_sa_dev.json"));
        GoogleCloudStorageBackend backend = new GoogleCloudStorageBackend(configuration);

        try (InputStream is = Files.newInputStream(Path.of("src/test/resources/1031-bytes.zip"))) {
            backend.write("gs://ssb-rawdata-dev/tmp/test.zip", is);
        }
    }

    @Disabled("This test runs against gcs")
    @Test
    void thatWriteWorksWithFlowable() throws IOException {
        Configuration configuration = new Configuration();
        configuration.setServiceAccountCredentials(Path.of("gcs-secret/gcs_sa_dev.json"));
        GoogleCloudStorageBackend backend = new GoogleCloudStorageBackend(configuration);

        try (InputStream is = Files.newInputStream(Path.of("src/test/resources/1031-bytes.zip"))) {

            Completable completable = backend.write(
                    "gs://ssb-rawdata-dev/tmp/test-flowable.zip",
                    Bytes.from(is, 256)
            ).timeout(10, TimeUnit.SECONDS);

            completable.blockingAwait();
        }
    }
}
