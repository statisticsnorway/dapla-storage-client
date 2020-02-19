package no.ssb.dapla.storage.client;

/**
 * Simple cursor representation.
 *
 * @param <T> the type of the cursor
 */
public class Cursor<T> {
    private final T after;
    private final Integer next;

    public Cursor(Integer next, T after) {
        this.after = after;
        this.next = next;
    }

    public T getAfter() {
        return after;
    }

    public Integer getNext() {
        return next;
    }
}
