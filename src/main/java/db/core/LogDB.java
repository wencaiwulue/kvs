package db.core;

import db.core.storage.StorageEngine;
import db.core.storage.impl.RocksDBStorage;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.LogEntry;
import raft.NodeAddress;
import util.FSTUtil;

import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

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
    private volatile long lastLogIndex;
    private final byte[] LAST_LOG_INDEX = "LAST_LOG_INDEX".getBytes();

    // key for storage currentTerm and lastVoteFor
    private final byte[] CURRENT_TERM = "CURRENT_TERM".getBytes();
    private final byte[] LAST_VOTE_FOR = "LAST_VOTE_FOR".getBytes();

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock readLock = this.lock.readLock();
    private final Lock writeLock = this.lock.writeLock();

    public LogDB(Path dir) {
        this.engine = new RocksDBStorage(dir);
        this.lastLogIndex = this.initLastLogIndex();
    }


    public LogEntry get(long key) {
        byte[] bytes = this.engine.get(String.valueOf(key).getBytes());
        if (bytes == null) {
            return null;
        }
        return (LogEntry) FSTUtil.getBinaryConf().asObject(bytes);
    }

    public int getLastLogTerm() {
        LogEntry logEntry = this.get(this.lastLogIndex);
        if (logEntry == null) {
            return 0;
        }
        return logEntry.getTerm();
    }

    //----------for storage currentTerm and lastVoteFor info start-------------
    public int getCurrentTerm() {
        byte[] bytes = this.engine.get(CURRENT_TERM);
        if (bytes == null || bytes.length == 0) {
            return 0;
        }
        return (int) FSTUtil.getBinaryConf().asObject(bytes);
    }

    public void setCurrentTerm(int currentTerm) {
        this.engine.set(CURRENT_TERM, FSTUtil.getBinaryConf().asByteArray(currentTerm));
    }

    public NodeAddress getLastVoteFor() {
        byte[] bytes = this.engine.get(LAST_VOTE_FOR);
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        return (NodeAddress) FSTUtil.getBinaryConf().asObject(bytes);
    }

    public void setLastVoteFor(NodeAddress lastVoteFor) {
        byte[] array = FSTUtil.getBinaryConf().asByteArray(lastVoteFor);
        this.engine.set(LAST_VOTE_FOR, array);
    }

    //----------for storage currentTerm and lastVoteFor info end-------------
    private long initLastLogIndex() {
        byte[] bytes = this.engine.get(LAST_LOG_INDEX);
        if (bytes == null || bytes.length == 0) {
            return 0;
        }
        return (long) FSTUtil.getBinaryConf().asObject(bytes);
    }

    public void saveLastLogIndex(long index) {
        byte[] array = FSTUtil.getBinaryConf().asByteArray(index);
        this.engine.set(LAST_LOG_INDEX, array);
    }

    public void save(List<LogEntry> logs) {
        for (LogEntry entry : logs) {
            this.set(entry.getIndex(), entry);
        }
    }

    public void set(long index, LogEntry logEntry) {
        if (logEntry == null) {
            return;
        }
        this.engine.set(String.valueOf(index).getBytes(), FSTUtil.getBinaryConf().asByteArray(logEntry));
        this.lastLogIndex = index;
        this.saveLastLogIndex(this.lastLogIndex);
    }

    public void removeRange(long indexInclusive, long indexExclusive) {
        this.engine.removeRange(String.valueOf(indexInclusive).getBytes(), String.valueOf(indexExclusive).getBytes());
    }

    public List<LogEntry> getRange(long fromIndexInclusive, long toIndexExclusive) {
        return LongStream.range(fromIndexInclusive, toIndexExclusive)
                .mapToObj(this::get)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }
}
