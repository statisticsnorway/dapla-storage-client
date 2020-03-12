package no.ssb.dapla.storage.client.backend;

import java.util.Objects;

public class FileInfo {
    private final String path;
    private long lastModified;
    private boolean isDirectory;

    public FileInfo(String path) {
        this.path = path;
    }

    public String getPath() {
        return path;
    }

    public long getLastModified() {
        return lastModified;
    }

    public boolean isDirectory() {
        return isDirectory;
    }

    public FileInfo setLastModified(long lastModified) {
        this.lastModified = lastModified;
        return this;
    }

    public FileInfo setDirectory(boolean directory) {
        isDirectory = directory;
        return this;
    }

    public boolean hasSuffix(String value) {
        Objects.requireNonNull(value);
        return path != null && path.endsWith(value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FileInfo fileInfo = (FileInfo) o;
        return Objects.equals(path, fileInfo.path);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path);
    }
}
