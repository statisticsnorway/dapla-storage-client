package no.ssb.dapla.storage.client.backend;

import java.util.Objects;

public class FileInfo {
    private final String name;
    private final String path;
    private final long lastModified;
    private final boolean isDirectory;

    public FileInfo(String name, String path, long lastModified, boolean isDirectory) {
        this.name = name;
        this.path = path;
        this.lastModified = lastModified;
        this.isDirectory = isDirectory;
    }

    public String getName() {
        return name;
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
