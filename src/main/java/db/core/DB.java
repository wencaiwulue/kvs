package db.core;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import db.config.Config;
import db.core.storage.MapStorage;
import db.core.storage.StorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.BackupUtil;
import util.FileUtil;
import util.KryoUtil;
import util.ThreadUtil;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
    private static final Logger log = LoggerFactory.getLogger(DB.class);

    private final CacheBuffer<CacheBuffer.Item> buffer = new CacheBuffer<>(Config.CACHE_SIZE, Config.CACHE_BACKUP_THRESHOLD);// 这里是缓冲区，也就是每隔一段时间备份append的数据，或者这个buffer满了就备份数据
    public final StorageEngine storage;

    public final PriorityBlockingQueue<ExpireKey> expireKeys;

    private volatile long lastAppendTime;
    private volatile long lastSnapshotTime;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock readLock = this.lock.readLock();
    private final Lock writeLock = this.lock.writeLock();
    public final Path dir;
    private final AtomicReference<MappedByteBuffer> lastModify = new AtomicReference<>();
    private final AtomicInteger fileNumber = new AtomicInteger(0);
    // 可以判断是否存在kvs中，但是不能删除，这点儿是不是不大靠谱, 实际上可以再加一个bitmap, 用于存储某个位被置1的次数，这样的方案解决
    @SuppressWarnings("UnstableApiUsage")
    private static final BloomFilter<String> BLOOM_FILTER = BloomFilter.create(Funnels.stringFunnel(StandardCharsets.UTF_8), Integer.MAX_VALUE);

    public DB(Path dir) {
        this.dir = dir;
        this.storage = new MapStorage();
        this.expireKeys = new PriorityBlockingQueue<>(11, ExpireKey::compareTo);
        this.initAndReadIntoMemory();
        this.checkExpireKey();
        this.writeDataToDisk();
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    public void initAndReadIntoMemory() {
        this.writeLock.lock();
        try {
            File f = this.dir.toFile();
            if (!f.exists()) FileUtil.emptyFolder(f);
            File[] files = f.listFiles();
            List<File> dbFiles = new ArrayList<>();

            if (files == null || files.length == 0) {
                File temp = Path.of(f.getPath(), fileNumber.getAndIncrement() + ".db").toFile();
                if (!temp.exists()) temp.createNewFile();
                dbFiles.add(temp);
            } else {
                dbFiles.addAll(Arrays.stream(files).collect(Collectors.toList()));
            }

            File file = dbFiles.get(dbFiles.size() - 1);
            this.fileNumber.set(dbFiles.size() - 1);// 要注意磁盘可能已经有数据块了
            this.lastModify.set(BackupUtil.fileMapped(file));

            for (File dbFile : dbFiles) {
                BackupUtil.readFromDisk(this.storage, dbFile);
            }
        } catch (IOException e) {
            log.error(e.getMessage());
        } finally {
            this.writeLock.unlock();
        }
    }

    public void writeDataToDisk() {
        Runnable backupTask = () -> {
            if (Config.BACKUP_MODE == 0 || Config.BACKUP_MODE == 2) {
                if (this.buffer.shouldToWriteOut() || this.shouldToAppend()) {
                    this.writeLock.lock();
                    try {
                        BackupUtil.appendToDisk(this.buffer, this.buffer.theNumberOfShouldToBeWritten(), this.dir, this.lastModify, this.fileNumber);
                    } finally {
                        this.writeLock.unlock();
                    }
                    this.lastAppendTime = System.currentTimeMillis();
                }
            }

            if (Config.BACKUP_MODE == 1 || Config.BACKUP_MODE == 2) {
                if (this.shouldToSnapshot()) {
                    this.lock.writeLock().lock();
                    try {
                        BackupUtil.snapshotToDisk(this.storage, this.dir, this.lastModify, this.fileNumber);
                    } finally {
                        this.lock.writeLock().unlock();
                    }
                    this.lastSnapshotTime = System.currentTimeMillis();
                }
            }
        };
        ThreadUtil.getScheduledThreadPool().scheduleAtFixedRate(backupTask, 0, Config.APPEND_RATE.toNanos() / 2, TimeUnit.NANOSECONDS);// 没半秒检查一次
    }

    // check key expire every seconds
    public void checkExpireKey() {
        Runnable checkExpireTask = () -> {
            while (!this.expireKeys.isEmpty()) {
                ExpireKey expireKey = this.expireKeys.peek();
                if (expireKey.isExpired()) {
                    this.writeLock.lock();
                    try {
                        this.storage.remove(expireKey.getKey());
                        this.expireKeys.poll();
                    } finally {
                        this.writeLock.unlock();
                    }
                } else {
                    break;
                }
            }
        };

        ThreadUtil.getScheduledThreadPool().scheduleAtFixedRate(checkExpireTask, 0, 1, TimeUnit.SECONDS);
    }

    public boolean shouldToSnapshot() {
        return this.lastSnapshotTime + Config.SNAPSHOT_RATE.toMillis() < System.currentTimeMillis();
    }

    public boolean shouldToAppend() {
        return this.lastAppendTime + Config.APPEND_RATE.toMillis() < System.currentTimeMillis();
    }


    public Object get(String key) {
        this.readLock.lock();
        try {
            return this.storage.get(key);
        } finally {
            this.readLock.unlock();
        }
    }

    public void set(String key, Object value) {
        set(key, value, 0, TimeUnit.MILLISECONDS);
    }

    public void set(String key, Object value, int timeout, TimeUnit unit) {
        this.writeLock.lock();
        try {
            if (key == null || value == null) return;
            this.storage.set(key, value);
            if (timeout > 0) {
                this.expireKeys.add(new ExpireKey(key, timeout, unit));
            }
            byte[] kb = key.getBytes();
            byte[] vb = KryoUtil.asByteArray(value);
            this.buffer.offer(new CacheBuffer.Item(kb, vb));
        } finally {
            this.writeLock.unlock();
        }
    }

    public void remove(String key) {
        this.writeLock.unlock();
        try {
            this.storage.remove(key);// expire key 可以不用删除
        } finally {
            this.writeLock.unlock();
        }
    }

    public void expireKey(String key, int expire, TimeUnit unit) {
        this.writeLock.lock();
        try {
            this.expireKeys.remove(new ExpireKey(key, -1, unit));
            this.expireKeys.add(new ExpireKey(key, expire, unit));
        } finally {
            this.writeLock.unlock();
        }
    }

}
