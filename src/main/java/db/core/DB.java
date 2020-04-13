package db.core;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import util.BackupUtil;
import util.ByteArrayUtil;
import util.FSTUtil;
import util.ThreadUtil;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
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
    private static final Logger log = LogManager.getLogger(DB.class);

    private int size = 1000 * 1000;// 缓冲区大小
    private int thresholdStart = (int) (size * 0.8);// 如果超过这个阈值，就启动备份写入流程
    private int thresholdStop = (int) (size * 0.2); // 低于这个值就停止写入，因为管道是不停写入的，所以基本不会出现管道为空的情况
    private ArrayDeque<byte[]> buffer = new ArrayDeque<>(size);// 这里是缓冲区，也就是每隔一段时间备份append的数据，或者这个buffer满了就备份数据

    private byte mode = 2; // 备份方式为增量还是快照，或者是混合模式, 0--append, 1--snapshot, 2--append+snapshot
    private volatile long lastAppendBackupTime;
    private final long appendRate = 1000 * 1000; // 每一秒append一次
    private volatile long lastSnapshotBackupTime;
    private final long snapshotRate = 60 * appendRate; // 每一分钟snapshot一下

    private ConcurrentHashMap<String, Object> map;// RockDB or LevelDB?
    private MinPQ<ExpireKey> expireKeys;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock readLock = this.lock.readLock();
    private final Lock writeLock = this.lock.writeLock();
    private String dbPath;
    private RandomAccessFile raf;
    // 可以判断是否存在kvs中，
    @SuppressWarnings("all")
    private static BloomFilter<String> filter = BloomFilter.create(Funnels.stringFunnel(StandardCharsets.UTF_8), Integer.MAX_VALUE);

    public DB(String dbPath) {
        this.map = new ConcurrentHashMap<>(/*1 << 30*/); // 这是hashMap的容量
        this.expireKeys = new MinPQ<>();
        this.dbPath = dbPath;
        initAndReadIntoMemory();
        checkExpireKey();
        writeDataToDisk();
    }

    @SuppressWarnings("all")
    public void initAndReadIntoMemory() {
        writeLock.lock();
        try {
            if (raf == null) {
                File f = new File(dbPath);
                if (!f.exists()) f.createNewFile();
                raf = new RandomAccessFile(f, "rw");
            }
            BackupUtil.readFromDisk(map, raf);
        } catch (IOException e) {
            log.error(e);
        } finally {
            writeLock.unlock();
        }
    }

    private void writeDataToDisk() {
        Runnable backup = () -> {
            int size = buffer.size();
            if (mode == 0 || mode == 2) {
                if (size > thresholdStart || lastAppendBackupTime + appendRate < System.nanoTime()) {
                    writeLock.lock();
                    try {
//                        BackupUtil.appendToDisk(buffer, size - thresholdStop, raf);
                    } finally {
                        writeLock.unlock();
                    }
                    lastAppendBackupTime = System.nanoTime();
                }
            }

            if (mode == 1 || mode == 2) {
                if (lastSnapshotBackupTime + snapshotRate < System.nanoTime()) {
                    lock.writeLock().lock();
                    try {
//                        BackupUtil.snapshotToDisk(map, raf);
                    } finally {
                        lock.writeLock().unlock();
                    }
                    lastSnapshotBackupTime = System.nanoTime();
                }
            }
        };
        ThreadUtil.getScheduledThreadPool().scheduleAtFixedRate(backup, 0, appendRate / 2, TimeUnit.NANOSECONDS);// 没半秒检查一次
    }

    // check key expire every seconds
    private void checkExpireKey() {
        Runnable check = () -> {
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

        ThreadUtil.getScheduledThreadPool().scheduleAtFixedRate(check, 0, 1, TimeUnit.SECONDS);
    }

    public Object get(String key) {
        this.readLock.lock();
        try {
            return map.get(key);
        } finally {
            this.readLock.unlock();
        }
    }

    public void set(String key, Object value) {
        set(key, value, 0, TimeUnit.MILLISECONDS);
    }

    public void set(String key, Object value, int timeout, TimeUnit unit) {
        if (key == null) return;
        map.put(key, value);
        if (timeout > 0) {
            expireKeys.insert(new ExpireKey(key, System.nanoTime() + unit.toNanos(timeout)));
        }
        byte[] kb = key.getBytes();
        byte[] vb = FSTUtil.getConf().asByteArray(value);
        buffer.push(ByteArrayUtil.combine(kb, vb));
    }

    public void remove(String key) {
        this.writeLock.unlock();
        try {
            map.remove(key);// expire key 可以不用删除
        } finally {
            this.writeLock.unlock();
        }
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
