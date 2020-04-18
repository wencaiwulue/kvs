package db.core;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import raft.LogEntry;
import util.*;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayDeque;
import java.util.List;
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
public class LogDB {
    private static final Logger log = LogManager.getLogger(LogDB.class);

    private final int size = 1000 * 1000;// 缓冲区大小
    private final int thresholdStart = (int) (size * 0.8);// 如果超过这个阈值，就启动备份写入流程
    private final int thresholdStop = (int) (size * 0.2); // 低于这个值就停止写入，因为管道是不停写入的，所以基本不会出现管道为空的情况
    private final ArrayDeque<byte[]> buffer = new ArrayDeque<>(size);// 这里是缓冲区，也就是每隔一段时间备份append的数据，或者这个buffer满了就备份数据

    private final byte mode = 2; // 备份方式为增量还是快照，或者是混合模式, 0--append, 1--snapshot, 2--append+snapshot
    private volatile long lastAppendBackupTime;
    private final long appendRate = 1000 * 1000; // 每一秒append一次
    private volatile long lastSnapshotBackupTime;
    private final long snapshotRate = 60 * appendRate; // 每一分钟snapshot一下

    private final ConcurrentHashMap<String, Object> map;// RockDB or LevelDB?
    public volatile int lastLogIndex;
    public volatile int lastLogTerm;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock readLock = this.lock.readLock();
    private final Lock writeLock = this.lock.writeLock();

    private final String logDBPath;
    private RandomAccessFile raf;

    public LogDB(String logDBPath) {
        this.map = new ConcurrentHashMap<>(/*1 << 30*/); // 这是hashMap的容量
        this.logDBPath = logDBPath;
        initAndReadIntoMemory();
        writeDataToDisk();
    }

    @SuppressWarnings("all")
    public void initAndReadIntoMemory() {
        writeLock.lock();
        try {
            if (raf == null) {
                File f = new File(logDBPath);
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

    public Object get(String key) {
        return map.get(key);
    }

    public void save(List<LogEntry> logs) {
        save(logs, false);
    }

    public void save(List<LogEntry> logs, boolean flush) {
        for (LogEntry entry : logs) {
            set(String.valueOf(entry.getIndex()), entry);
        }
        if (flush) {
            writeDataToDisk();
        }
    }

    public void set(String key, Object value) {
        if (key == null) return;
        map.put(key, value);
        byte[] kb = key.getBytes();
        byte[] vb = KryoUtil.asByteArray(value);
        buffer.push(ByteArrayUtil.combine(kb, vb));
    }

    public void remove(String key) {
        map.remove(key);
    }
}
