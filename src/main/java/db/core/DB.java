package db.core;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import util.BackupUtil;
import util.KryoUtil;
import util.ThreadUtil;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

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

    private final int size = 1000 * 1000;
    private final CacheBuffer<CacheBuffer.Item> buffer = new CacheBuffer<>(size, 0.2D, 0.8D);// 这里是缓冲区，也就是每隔一段时间备份append的数据，或者这个buffer满了就备份数据
    public final Storage storage;

    private final byte mode; // 备份方式为增量还是快照，或者是混合模式, 0--append, 1--snapshot, 2--append+snapshot
    private volatile long lastAppendBackupTime;
    private final long appendRate = 1000 * 1000; // 每一秒append一次
    private volatile long lastSnapshotBackupTime;
    private final long snapshotRate = 60 * appendRate; // 每一分钟snapshot一下
    private final int maxFileSize = Integer.MAX_VALUE; // 大概文件大小为2gb

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock readLock = this.lock.readLock();
    private final Lock writeLock = this.lock.writeLock();
    public final Path dbDir;
    private final AtomicReference<MappedByteBuffer> haveFreeSpaceFile = new AtomicReference<>();
    // 可以判断是否存在kvs中，
    @SuppressWarnings("UnstableApiUsage")
    private static final BloomFilter<String> filter = BloomFilter.create(Funnels.stringFunnel(StandardCharsets.UTF_8), Integer.MAX_VALUE);

    public DB(String dbDir) {
        this.dbDir = Path.of(dbDir);
        this.storage = new Storage(this.dbDir, this.writeLock, this.haveFreeSpaceFile);
        this.mode = 2;
        this.initAndReadIntoMemory();
        this.checkExpireKey();
        this.writeDataToDisk();
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    public void initAndReadIntoMemory() {
        this.writeLock.lock();
        try {
            File f = this.dbDir.toFile();
            if (!f.exists()) f.createNewFile();
            File[] files = f.listFiles();
            List<File> dbFiles = new ArrayList<>();

            if (files == null || files.length == 0) {
                File temp = Path.of(f.getPath(), 1 + ".db").toFile();
                if (!temp.exists()) temp.createNewFile();
                dbFiles.add(temp);
            } else {
                dbFiles.addAll(Arrays.stream(files).collect(Collectors.toList()));
            }

            File file = dbFiles.get(dbFiles.size() - 1);
            this.haveFreeSpaceFile.set(Storage.getMappedByteBuffer(file));

            for (File dbFile : dbFiles) {
                BackupUtil.readFromDisk(this.storage.map, dbFile);
            }
        } catch (IOException e) {
            log.error(e);
        } finally {
            this.writeLock.unlock();
        }
    }

    public void writeDataToDisk() {
        Runnable backup = () -> {
            if (this.mode == 0 || this.mode == 2) {
                if (this.buffer.shouldToWriteOut() || this.lastAppendBackupTime + this.appendRate < System.nanoTime()) {
                    this.writeLock.lock();
                    try {
                        BackupUtil.appendToDisk(this.buffer, this.buffer.theNumberOfShouldToBeWritten(), this.haveFreeSpaceFile, this.maxFileSize);
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
                        this.storage.snapshotRapidly();
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
            while (!this.storage.expireKeys.isEmpty()) {
                ExpireKey expireKey = this.storage.expireKeys.peek();
                if (System.nanoTime() >= expireKey.getExpire()) {
                    this.writeLock.lock();
                    try {
                        this.storage.map.remove(expireKey.getKey());
                        this.storage.expireKeys.poll();
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
            return this.storage.map.get(key);
        } finally {
            this.readLock.unlock();
        }
    }

    public void set(String key, Object value) {
        set(key, value, 0, TimeUnit.MILLISECONDS);
    }

    public void set(String key, Object value, int timeout, TimeUnit unit) {
        if (key == null || value == null) return;
        this.storage.map.put(key, value);
        if (timeout > 0) {
            this.storage.expireKeys.add(new ExpireKey(key, System.nanoTime() + unit.toNanos(timeout)));
        }
        byte[] kb = key.getBytes();
        byte[] vb = KryoUtil.asByteArray(value);
        this.buffer.addLast(new CacheBuffer.Item(kb, vb));
    }

    public void remove(String key) {
        this.writeLock.unlock();
        try {
            this.storage.map.remove(key);// expire key 可以不用删除
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
