package db.core;

import com.google.common.collect.Range;
import lombok.Getter;

import java.util.ArrayDeque;

/**
 * @author naison
 * @since 4/28/2020 16:39
 */
@Getter
public class CacheBuffer<T> {
    private final int size;// 缓冲区大小
    // 如果超过这个阈值，就启动备份写入流程
    // 低于这个值就停止写入，因为管道是不停写入的，所以基本不会出现管道为空的情况
    private final Range<Integer> threshold;
    private final ArrayDeque<T> buffer;// 这里是缓冲区，也就是每隔一段时间备份append的数据，或者这个buffer满了就备份数据

    public CacheBuffer(int size, double lowerBound, double upperBound) {
        this.size = size;
        this.threshold = Range.openClosed((int) (size * lowerBound), (int) (size * upperBound));
        this.buffer = new ArrayDeque<>(size);
    }

    public boolean shouldToWriteOut() {
        return this.buffer.size() >= this.threshold.upperEndpoint();
    }

    public int theNumberOfShouldToBeWritten() {
        return Math.max(this.buffer.size() - this.threshold.lowerEndpoint(), 0);
    }

    public T peekLast() {
        return this.buffer.peekLast();
    }

    public T pollLast() {
        return this.buffer.peekLast();
    }

    public void addLast(T t) {
        this.buffer.addLast(t);
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
