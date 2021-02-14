package db.core.pojo;

import com.google.common.collect.Range;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author naison
 * @since 4/28/2020 16:39
 */
public class CacheBuffer<T> {
    // if out of range, start write out
    // if blow the range, stop write out
    private final Range<Integer> threshold;
    // cache, schedule to flush data to disk, or if this cache is full to flush data to disk
    private final BlockingQueue<T> buffer;

    public CacheBuffer(int size, Range<Integer> range) {
        this.threshold = range;
        this.buffer = new LinkedBlockingQueue<>(size);
    }

    public boolean shouldToWriteOut() {
        return this.buffer.size() >= this.threshold.upperEndpoint();
    }

    public int theNumberOfShouldToBeWritten() {
        return Math.max(0, this.buffer.size() - this.threshold.lowerEndpoint());
    }

    public T peek() {
        return this.buffer.peek();
    }

    public T poll() {
        return this.buffer.poll();
    }

    public void offer(T t) {
        this.buffer.offer(t);
    }

    public boolean isEmpty() {
        return this.buffer.isEmpty();
    }


    public static class Item {
        public byte[] key;
        public byte[] value;
        public int size;

        public Item(byte[] key, byte[] value) {
            this.key = key;
            this.value = value;
            this.size = key.length + value.length;
        }

        public int getSize() {
            return size;
        }
    }
}
