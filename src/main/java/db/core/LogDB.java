package db.core;

import com.google.common.collect.Range;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import raft.LogEntry;
import util.BackupUtil;
import util.ByteArrayUtil;
import util.KryoUtil;
import util.ThreadUtil;

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
    // 如果超过这个阈值，就启动备份写入流程
    // 低于这个值就停止写入，因为管道是不停写入的，所以基本不会出现管道为空的情况
    private final Range<Integer> threshold = Range.openClosed((int) (size * 0.2), (int) (size * 0.8));
    private final ArrayDeque<byte[]> buffer = new ArrayDeque<>(size);// 这里是缓冲区，也就是每隔一段时间备份append的数据，或者这个buffer满了就备份数据

    private volatile long lastAppendBackupTime;
    private final long appendRate = 1000 * 1000; // 每一秒append一次

    private final ConcurrentHashMap<String, Object> map;
    public volatile int lastLogIndex;
    public volatile int lastLogTerm;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock readLock = this.lock.readLock();
    private final Lock writeLock = this.lock.writeLock();

    public final String logDBPath;
    public RandomAccessFile raf;

    public LogDB(String logDBPath) {
        this.map = new ConcurrentHashMap<>(/*1 << 30*/); // 这是hashMap的容量
        this.logDBPath = logDBPath;
        initAndReadIntoMemory();
        writeDataToDisk();
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    public void initAndReadIntoMemory() {
        this.writeLock.lock();
        try {
            if (this.raf == null) {
                File f = new File(this.logDBPath);
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
            if (size > this.threshold.upperEndpoint() || this.lastAppendBackupTime + this.appendRate < System.nanoTime()) {
                this.writeLock.lock();
                try {
                    BackupUtil.appendToDisk(this.buffer, size - this.threshold.lowerEndpoint(), this.raf);
                } finally {
                    this.writeLock.unlock();
                }
                this.lastAppendBackupTime = System.nanoTime();
            }
        };
        ThreadUtil.getScheduledThreadPool().scheduleAtFixedRate(backup, 0, this.appendRate / 2, TimeUnit.NANOSECONDS);// 没半秒检查一次
    }

    public Object get(String key) {
        return this.map.get(key);
    }

    public void save(List<LogEntry> logs) {
        this.save(logs, false);
    }

    public void save(List<LogEntry> logs, boolean flush) {
        for (LogEntry entry : logs) {
            this.set(String.valueOf(entry.getIndex()), entry);
        }
        if (flush) {
            this.writeDataToDisk();
        }
    }

    public void set(String key, Object value) {
        if (key == null) return;
        this.map.put(key, value);
        byte[] kb = key.getBytes();
        byte[] vb = KryoUtil.asByteArray(value);
        this.buffer.push(ByteArrayUtil.combineKeyVal(kb, vb));
    }

    public void remove(String key) {
        this.map.remove(key);
    }
}
