package no.ssb.dapla.storage.client;

import org.apache.parquet.example.data.simple.SimpleGroup;

public interface ParquetGroupVisitor {
    void visit(SimpleGroup value);
}
