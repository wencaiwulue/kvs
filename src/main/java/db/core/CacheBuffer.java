package db.core;

import com.google.common.collect.Range;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author naison
 * @since 4/28/2020 16:39
 */
public class CacheBuffer<T> {
    // 如果超过这个阈值，就启动备份写入流程
    // 低于这个值就停止写入，因为管道是不停写入的，所以基本不会出现管道为空的情况
    private final Range<Integer> threshold;
    private final BlockingQueue<T> buffer;// 这里是缓冲区，也就是每隔一段时间备份append的数据，或者这个buffer满了就备份数据

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
