package no.ssb.dapla.storage.client;

import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.filter.RecordFilter;
import org.apache.parquet.filter.UnboundRecordFilter;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Copy of the version in https://github.com/apache/parquet-mr/pull/631
 * TODO: Remove when merged.
 */
public final class PagedRecordFilter implements UnboundRecordFilter {

    private final long startPos;
    private final long endPos;
    private AtomicLong currentPos = new AtomicLong(0);

    public PagedRecordFilter(long startPos, long endPos) {
        this.startPos = startPos;
        this.endPos = endPos;
    }

    @Override
    public RecordFilter bind(Iterable<ColumnReader> readers) {
        return new BoundedPagedRecordFilter();
    }

    private class BoundedPagedRecordFilter implements RecordFilter {

        @Override
        public boolean isMatch() {
            long pos = currentPos.incrementAndGet();
            return ((pos >= startPos) && (pos < endPos));
        }
    }
}
