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

    /**
     * Write a series of bytes to a file. The file will be created if it doesn't already exist, and if the file already
     * exists any bytes within will be overwritten.
     *
     * @param path a full file path, including schema, e.g gs://storage-bucket/a-file.txt
     * @param content the content as an array of bytes
     */
    void write(String path, byte[] content) throws IOException;

    void move(String from, String to) throws IOException;

    void delete(String path) throws IOException;

    /** Return FileInfo items ordered by specified comparators */
    Flowable<FileInfo> list(String path, Comparator<FileInfo>... comparators) throws IOException;

    /** Return FileInfo items ordered alphabetically by path */
    default Flowable<FileInfo> list(String path) throws IOException {
        return list(path, Comparator.comparing(FileInfo::getPath));
    }

}
