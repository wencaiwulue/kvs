package db.core.storage.impl;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import db.config.Config;
import db.core.pojo.CacheBuffer;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * @author naison
 * @since 4/28/2020 18:03
 */

public class MapStorage<K, V> implements StorageEngine<K, V> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MapStorage.class);

    private final Path dir;
    public final ConcurrentHashMap<K, V> map;
    private final CacheBuffer<CacheBuffer.Item> buffer = new CacheBuffer<>(Config.CACHE_SIZE, Config.CACHE_BACKUP_THRESHOLD);// 这里是缓冲区，也就是每隔一段时间备份append的数据，或者这个buffer满了就备份数据
    private volatile long lastAppendTime;
    private volatile long lastSnapshotTime;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock readLock = this.lock.readLock();
    private final Lock writeLock = this.lock.writeLock();
    private final AtomicReference<MappedByteBuffer> lastModify = new AtomicReference<>();
    private final AtomicInteger fileNumber = new AtomicInteger(0);
    // 可以判断是否存在kvs中，但是不能删除，这点儿是不是不大靠谱, 实际上可以再加一个bitmap, 用于存储某个位被置1的次数，这样的方案解决
    @SuppressWarnings("UnstableApiUsage")
    private static final BloomFilter<String> BLOOM_FILTER = BloomFilter.create(Funnels.stringFunnel(StandardCharsets.UTF_8), Integer.MAX_VALUE);

    public MapStorage(Path dir) {
        this.dir = dir;
        // 1 << 30 is hashmap max capacity
        this.map = new ConcurrentHashMap<>(1 << 30);
        this.initAndReadIntoMemory();
        this.writeDataToDisk();
    }


    public boolean shouldToSnapshot() {
        return this.lastSnapshotTime + Config.SNAPSHOT_RATE.toMillis() < System.currentTimeMillis();
    }

    public boolean shouldToAppend() {
        return this.lastAppendTime + Config.APPEND_RATE.toMillis() < System.currentTimeMillis();
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
                BackupUtil.readFromDisk(this, dbFile);
            }
        } catch (IOException e) {
            LOGGER.error(e.getMessage());
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
                        BackupUtil.snapshotToDisk(this, this.dir, this.lastModify, this.fileNumber);
                    } finally {
                        this.lock.writeLock().unlock();
                    }
                    this.lastSnapshotTime = System.currentTimeMillis();
                }
            }
        };
        ThreadUtil.getScheduledThreadPool().scheduleAtFixedRate(backupTask, 0, Config.APPEND_RATE.toNanos() / 2, TimeUnit.NANOSECONDS);// 没半秒检查一次
    }

    @Override
    public V get(K key) {
        return this.map.get(key);
    }

    @Override
    public boolean set(K key, V v) {
        this.map.put(key, v);
        byte[] kb = KryoUtil.asByteArray(key);
        byte[] vb = KryoUtil.asByteArray(v);
        this.buffer.offer(new CacheBuffer.Item(kb, vb));
        return true;
    }

    @Override
    public boolean remove(K key) {
        this.map.remove(key);
        return true;
    }

    public Iterator<Map.Entry<K, V>> iterator() {
        return map.entrySet().iterator();
    }

}
