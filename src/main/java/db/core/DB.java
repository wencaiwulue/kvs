package db.core;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import thread.FSTUtil;
import thread.ThreadUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 设计目标（每秒）
 * 写入：十万次
 * 读取：二十万次
 *
 * @author naison
 * @since 4/1/2020 10:53
 */
public class DB {
    private int size = 1000 * 1000;// 缓冲区大小
    private int threshold = (int) (size * 0.8);// 如果超过这个阈值，就启动备份写入流程
    private int thresholdStop = (int) (size * 0.2); // 低于这个值就停止写入
    private ArrayDeque<byte[]> buffer = new ArrayDeque<>(size);// 这里是缓冲区，也就是每隔一段时间备份append的数据，或者这个buffer满了就备份数据

    private byte mode = 1; // 备份方式为增量还是快照，或者是混合模式
    private volatile long lastAppendBackupTime;
    private final long appendRate = 1000 * 1000; // 每一秒append一次
    private volatile long lastSnapshotBackupTime;
    private final long snapshotRate = 60 * appendRate; // 每一分钟snapshot一下

    private String path;
    private ConcurrentHashMap<String, Object> map;
    private MinimalPriorityQueue<ExpireKey> expireKeys;
    private static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    // 可以判断是否存在kvs中，
    @SuppressWarnings("all")
    private static BloomFilter<String> filter = BloomFilter.create(Funnels.stringFunnel(StandardCharsets.UTF_8), Integer.MAX_VALUE);

    public DB(String fileSuffix) {
        this.map = new ConcurrentHashMap<>();
        this.expireKeys = new MinimalPriorityQueue<>();
        this.path = "C:\\Users\\89570\\Documents\\kvs_" + fileSuffix + "_data.db";
        checkExpireKey();
        writeDataToDisk();
    }

    private void writeDataToDisk() {
        Runnable r = () -> {
            try {
                int size = buffer.size();
                if (size > threshold || lastAppendBackupTime + appendRate < System.nanoTime()) {
                    BackupUtil.appendToDisk(buffer, size - thresholdStop, this.path);
                    lastAppendBackupTime = System.nanoTime();
                } else if (lastSnapshotBackupTime + snapshotRate < System.nanoTime()) {
                    BackupUtil.snapshotToDisk(map, this.path);
                    lastSnapshotBackupTime = System.nanoTime();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        };
        ThreadUtil.getSchedulePool().scheduleAtFixedRate(r, 0, 500, TimeUnit.MILLISECONDS);
    }

    // check key expire every seconds
    private void checkExpireKey() {
        Runnable runnable = () -> {
            ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
            while (!expireKeys.isEmpty()) {
                ExpireKey expireKey = expireKeys.peekMin();
                if (System.nanoTime() >= expireKey.getExpire()) {
                    writeLock.lock();
                    try {
                        map.remove(expireKey.getKey());
                        expireKeys.delMin();
                    } finally {
                        writeLock.unlock();
                    }
                } else {
                    break;
                }
            }
        };

        ThreadUtil.getSchedulePool().scheduleAtFixedRate(runnable, 0, 1, TimeUnit.SECONDS);
    }

    public Object get(String key) {
        return map.get(key);
    }

    public void set(String key, Object value) {
        set(key, value, 0, TimeUnit.MILLISECONDS);
    }

    private static byte[] combine(byte[]... keyValBytes) {
        int l = keyValBytes.length * 4;
        for (byte[] value : keyValBytes) {
            l += value.length;
        }

        ByteBuffer byteBuffer = ByteBuffer.allocate(l);
        for (byte[] aByte : keyValBytes) {
            byteBuffer.putInt(aByte.length);
            byteBuffer.put(aByte);
        }

        return byteBuffer.array();
    }

    public void set(String key, Object value, int timeout, TimeUnit unit) {
        if (key == null) return;
        map.put(key, value);
        if (timeout > 0) {
            expireKeys.insert(new ExpireKey(key, System.nanoTime() + unit.toNanos(timeout)));
        }
        byte[] kb = key.getBytes();
        byte[] vb = FSTUtil.getConf().asByteArray(value);
        buffer.push(combine(kb, vb));
    }

    public void remove(String key) {
        map.remove(key);// expire key 可以不用删除
    }

    public void expireKey(String key, int expire, TimeUnit unit) {
        // 如果已经在expireKey中，就会出问题。处理方法是加反向索引，但是这样值得吗
    }

    public static void main(String[] args) throws InterruptedException {
        DB db = new DB("");
        for (int i = 4; i > 0; i--) {
            db.set(String.valueOf(i), i, i, TimeUnit.NANOSECONDS);
        }
        Thread.sleep(1000);
        System.out.println(db);
    }

}
