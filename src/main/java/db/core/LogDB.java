package db.core;

import db.core.storage.StorageEngine;
import db.core.storage.impl.RocksDBStorage;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.LogEntry;
import util.FSTUtil;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author naison
 * @since 4/1/2020 10:53
 */
@Getter
@Setter
@ToString
public class LogDB {
    private static final Logger LOG = LoggerFactory.getLogger(LogDB.class);

    private final StorageEngine<byte[], byte[]> engine;
    private volatile int lastLogIndex;
    private volatile int lastLogTerm;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock readLock = this.lock.readLock();
    private final Lock writeLock = this.lock.writeLock();

    public LogDB(Path dir) {
        this.engine = new RocksDBStorage(dir);
    }


    public Object get(String key) {
        return this.engine.get(key.getBytes());
    }

    public void save(List<LogEntry> logs) {
        for (LogEntry entry : logs) {
            this.set(String.valueOf(entry.getIndex()), entry);
        }
    }

    public void set(String key, Object value) {
        if (key == null || value == null) {
            return;
        }
        this.engine.set(key.getBytes(), FSTUtil.getJsonConf().asByteArray(value));
    }

    public void remove(String key) {
        this.engine.remove(key.getBytes());
    }
}
