package no.ssb.dapla.storage.client.backend.local;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.assertj.core.api.Assertions.assertThat;

class LocalBackendTest {

    private Path testDir;

    @BeforeEach
    void setUp() throws IOException {
        testDir = Files.createTempDirectory("LocalBackendTest");
    }

    @AfterEach
    void tearDown() throws IOException {
        FileUtils.deleteDirectory(testDir.toFile());
    }

    @Test
    void thatWriteWorksWhenFileExists() throws IOException {
        Path file = testDir.resolve("a-file");
        Files.write(file, "first line\n".getBytes(), StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
        LocalBackend backend = new LocalBackend();
        backend.write(file.toUri().toString(), "second line".getBytes());
        assertThat(Files.readAllBytes(file)).isEqualTo("second line".getBytes());
    }

    @Test
    void thatWriteWorksWhenFileDoesntExist() throws IOException {
        LocalBackend backend = new LocalBackend();
        Path file = testDir.resolve("write-to-this");
        backend.write(file.toUri().toString(), "first\nsecond\n".getBytes());
        assertThat(Files.readAllBytes(file)).isEqualTo("first\nsecond\n".getBytes());
    }
}
