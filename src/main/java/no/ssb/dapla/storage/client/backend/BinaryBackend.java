package no.ssb.dapla.storage.client.backend;

import io.reactivex.Flowable;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.util.Comparator;

/**
 * Binary file system abstraction.
 */
public interface BinaryBackend {

    SeekableByteChannel read(String path) throws IOException;

    SeekableByteChannel write(String path) throws IOException;

    void move(String from, String to) throws IOException;

    void delete(String path) throws IOException;

    /** Return FileInfo items ordered by specified comparators */
    Flowable<FileInfo> list(String path, Comparator<FileInfo>... comparators) throws IOException;

    /** Return FileInfo items ordered alphabetically by path */
    default Flowable<FileInfo> list(String path) throws IOException {
        return list(path, Comparator.comparing(FileInfo::getPath));
    }

}
