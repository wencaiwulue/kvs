package db.core;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import db.core.pojo.ExpireKey;
import db.core.storage.impl.RocksDBStorage;
import db.core.storage.StorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.FSTUtil;
import util.ThreadUtil;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * design goal
 * write：10k TPS
 * read：20k QPS
 *
 * @author naison
 * @since 4/1/2020 10:53
 */
public class DB {
    private static final Logger log = LoggerFactory.getLogger(DB.class);

    public final StorageEngine<byte[], byte[]> storage;

    public final DelayQueue<ExpireKey> expireKeys;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock readLock = this.lock.readLock();
    private final Lock writeLock = this.lock.writeLock();
    // 可以判断是否存在kvs中，但是不能删除，这点儿是不是不大靠谱, 实际上可以再加一个bitmap, 用于存储某个位被置1的次数，这样的方案解决
    @SuppressWarnings("UnstableApiUsage")
    private static final BloomFilter<String> BLOOM_FILTER = BloomFilter.create(Funnels.stringFunnel(StandardCharsets.UTF_8), Integer.MAX_VALUE);

    public DB(Path dir) {
        this.storage = new RocksDBStorage(dir);
        this.expireKeys = new DelayQueue<>();
        this.checkExpireKey();
    }


    // check key expire every seconds
    public void checkExpireKey() {
        Runnable checkExpireTask = () -> {
            while (!this.expireKeys.isEmpty()) {
                ExpireKey expireKey = null;
                try {
                    expireKey = this.expireKeys.take();
                } catch (InterruptedException ignored) {
                }
                if (expireKey != null) {
                    this.writeLock.lock();
                    try {
                        this.storage.remove(expireKey.getKey().getBytes());
                    } finally {
                        this.writeLock.unlock();
                    }
                }
            }
        };

        ThreadUtil.getScheduledThreadPool().scheduleAtFixedRate(checkExpireTask, 0, 1, TimeUnit.SECONDS);
    }


    public Object get(String key) {
        this.readLock.lock();
        try {
            return this.storage.get(key.getBytes());
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
            this.storage.set(key.getBytes(), FSTUtil.getJsonConf().asByteArray(value));
            if (timeout > 0) {
                this.expireKeys.add(new ExpireKey(timeout, unit, key));
            }
        } finally {
            this.writeLock.unlock();
        }
    }

    public void remove(String key) {
        this.writeLock.unlock();
        try {
            this.storage.remove(key.getBytes());// expire key 可以不用删除
        } finally {
            this.writeLock.unlock();
        }
    }

    public void expireKey(String key, int expire, TimeUnit unit) {
        this.writeLock.lock();
        try {
            this.expireKeys.remove(new ExpireKey(-1, unit, key));
            this.expireKeys.add(new ExpireKey(expire, unit, key));
        } finally {
            this.writeLock.unlock();
        }
    }

}
