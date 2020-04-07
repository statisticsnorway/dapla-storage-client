package no.ssb.dapla.storage.client.backend.local;

import io.reactivex.Flowable;
import no.ssb.dapla.storage.client.backend.BinaryBackend;
import no.ssb.dapla.storage.client.backend.FileInfo;

import java.io.IOException;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LocalBackend implements BinaryBackend {

    @Override
    public Flowable<FileInfo> list(String path, Comparator<FileInfo>... comparators) throws IOException {
        Comparator<FileInfo> comparator = List.of(comparators).stream().reduce(Comparator::thenComparing).orElse(Comparator.comparing(FileInfo::getPath));

        try (Stream<Path> stream = Files.walk(Path.of(URI.create(path)), 1)) {
            return Flowable.fromIterable(stream
                    .map((Path::toFile))
                    .map(file -> new FileInfo(file.getName(), file.getPath(), file.lastModified(), file.isDirectory()))
                    .sorted(comparator)
                    .collect(Collectors.toList()));
        }
    }

    @Override
    public SeekableByteChannel read(String path) throws IOException {
        return Files.newByteChannel(Path.of(URI.create(path)), StandardOpenOption.READ);
    }

    @Override
    public SeekableByteChannel write(String path) throws IOException {
        return Files.newByteChannel(Path.of(URI.create(path)), StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
    }

    @Override
    public void move(String from, String to) throws IOException {
        Files.move(Path.of(URI.create(from)), Path.of(URI.create(to)), StandardCopyOption.ATOMIC_MOVE);
    }

    @Override
    public void delete(String path) throws IOException {
        Files.delete(Path.of(URI.create(path)));
    }
}
