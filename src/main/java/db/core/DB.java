package db.core;

import com.google.common.collect.Range;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import util.BackupUtil;
import util.ByteArrayUtil;
import util.KryoUtil;
import util.ThreadUtil;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
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

    private final int size = 1000 * 1000;// 缓冲区大小
    // 如果超过这个阈值，就启动备份写入流程
    // 低于这个值就停止写入，因为管道是不停写入的，所以基本不会出现管道为空的情况
    private final Range<Integer> threshold = Range.openClosed((int) (size * 0.2), (int) (size * 0.8));
    private final ArrayDeque<byte[]> buffer = new ArrayDeque<>(size);// 这里是缓冲区，也就是每隔一段时间备份append的数据，或者这个buffer满了就备份数据

    private final byte mode; // 备份方式为增量还是快照，或者是混合模式, 0--append, 1--snapshot, 2--append+snapshot
    private volatile long lastAppendBackupTime;
    private final long appendRate = 1000 * 1000; // 每一秒append一次
    private volatile long lastSnapshotBackupTime;
    private final long snapshotRate = 60 * appendRate; // 每一分钟snapshot一下

    private final ConcurrentHashMap<String, Object> map;// RockDB or LevelDB? B-tree?
    private final PriorityBlockingQueue<ExpireKey> expireKeys; // java.util.PriorityQueue ??
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock readLock = this.lock.readLock();
    private final Lock writeLock = this.lock.writeLock();
    public final String dbPath;
    private RandomAccessFile raf;
    // 可以判断是否存在kvs中，
    @SuppressWarnings("UnstableApiUsage")
    private static final BloomFilter<String> filter = BloomFilter.create(Funnels.stringFunnel(StandardCharsets.UTF_8), Integer.MAX_VALUE);

    public DB(String dbPath) {
        this.map = new ConcurrentHashMap<>(/*1 << 30*/); // 这是hashMap的容量
        this.expireKeys = new PriorityBlockingQueue<>(11, ExpireKey::compareTo);
        this.dbPath = dbPath;
        this.mode = 2;
        initAndReadIntoMemory();
        checkExpireKey();
        writeDataToDisk();
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    public void initAndReadIntoMemory() {
        this.writeLock.lock();
        try {
            if (this.raf == null) {
                File f = new File(this.dbPath);
                if (!f.exists()) f.createNewFile();
                this.raf = new RandomAccessFile(f, "rw");
            }
            BackupUtil.readFromDisk(this.map, this.raf);
        } catch (IOException e) {
            log.error(e);
        } finally {
            this.writeLock.unlock();
        }
    }

    public void writeDataToDisk() {
        Runnable backup = () -> {
            int size = this.buffer.size();
            if (this.mode == 0 || this.mode == 2) {
                if (size > this.threshold.upperEndpoint() || this.lastAppendBackupTime + this.appendRate < System.nanoTime()) {
                    this.writeLock.lock();
                    try {
                        BackupUtil.appendToDisk(this.buffer, size - this.threshold.lowerEndpoint(), this.raf);
                    } finally {
                        this.writeLock.unlock();
                    }
                    this.lastAppendBackupTime = System.nanoTime();
                }
            }

            if (this.mode == 1 || this.mode == 2) {
                if (this.lastSnapshotBackupTime + this.snapshotRate < System.nanoTime()) {
                    this.lock.writeLock().lock();
                    try {
                        BackupUtil.snapshotToDisk(this.map, this.raf);
                    } finally {
                        this.lock.writeLock().unlock();
                    }
                    this.lastSnapshotBackupTime = System.nanoTime();
                }
            }
        };
        ThreadUtil.getScheduledThreadPool().scheduleAtFixedRate(backup, 0, this.appendRate / 2, TimeUnit.NANOSECONDS);// 没半秒检查一次
    }

    // check key expire every seconds
    public void checkExpireKey() {
        Runnable check = () -> {
            while (!this.expireKeys.isEmpty()) {
                ExpireKey expireKey = this.expireKeys.peek();
                if (System.nanoTime() >= expireKey.getExpire()) {
                    this.writeLock.lock();
                    try {
                        this.map.remove(expireKey.getKey());
                        this.expireKeys.poll();
                    } finally {
                        this.writeLock.unlock();
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
        this.map.put(key, value);
        if (timeout > 0) {
            this.expireKeys.add(new ExpireKey(key, System.nanoTime() + unit.toNanos(timeout)));
        }
        byte[] kb = key.getBytes();
        byte[] vb = KryoUtil.asByteArray(value);
        this.buffer.push(ByteArrayUtil.combineKeyVal(kb, vb));
    }

    public void remove(String key) {
        this.writeLock.unlock();
        try {
            this.map.remove(key);// expire key 可以不用删除
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
