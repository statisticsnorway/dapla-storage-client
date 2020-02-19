package no.ssb.dapla.storage.client;

import io.reactivex.Flowable;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

public class LocalBackend implements BinaryBackend {

    private final String prefix;

    public LocalBackend(String prefix) {
        this.prefix = prefix;
    }

    @Override
    public Flowable<String> list(String path) throws IOException {
        return null;
    }

    @Override
    public SeekableByteChannel read(String path) throws FileNotFoundException {
        File file = new File(prefix + path);
        return new FileInputStream(file).getChannel();
    }

    @Override
    public SeekableByteChannel write(String path) throws IOException {
        File file = new File(prefix + path);
        File dir = file.getParentFile();
        if (!dir.exists() && !dir.mkdirs()) {
            throw new IOException("could not create " + dir);
        }
        if (!file.createNewFile()) {
            throw new IOException("file " + file + " already exist");
        }
        return new FileOutputStream(file).getChannel();
    }

    @Override
    public void move(String from, String to) throws IOException {
        File source = new File(prefix + from);
        Path destination = new File(prefix + to).toPath();
        Files.move(source.toPath(), destination, StandardCopyOption.ATOMIC_MOVE);
    }

    @Override
    public void delete(String path) throws IOException {
        File file = new File(prefix + path);
        Files.delete(file.toPath());
    }
}
