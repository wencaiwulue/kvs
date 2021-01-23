package db.core;

import db.core.storage.MapStorage;
import db.core.storage.StorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.LogEntry;
import util.BackupUtil;
import util.FileUtil;
import util.KryoUtil;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static util.BackupUtil.write;

/**
 * 设计目标（每秒）
 * 写入：十万次
 * 读取：二十万次
 *
 * @author naison
 * @since 4/1/2020 10:53
 */
public class LogDB {
    private static final Logger LOG = LoggerFactory.getLogger(LogDB.class);

    private final StorageEngine engine;
    public volatile int lastLogIndex;
    public volatile int lastLogTerm;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock readLock = this.lock.readLock();
    private final Lock writeLock = this.lock.writeLock();

    public final Path dir;
    public List<File> file;
    private final AtomicReference<MappedByteBuffer> lastModify = new AtomicReference<>();
    private final AtomicInteger fileNumber = new AtomicInteger(0);

    public LogDB(Path dir) {
        this.engine = new MapStorage(); // 这是hashMap的容量
        this.dir = dir;
        initAndReadIntoMemory();
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    public void initAndReadIntoMemory() {
        this.writeLock.lock();
        try {
            File f = this.dir.toFile();
            if (!f.exists()) {
                FileUtil.emptyFolder(f);
            }

            File[] files = f.listFiles();
            List<File> dbFiles = new ArrayList<>();

            if (files == null || files.length == 0) {
                File file = Path.of(f.getPath(), fileNumber.getAndIncrement() + ".db").toFile();
                if (!file.exists()) file.createNewFile();
                dbFiles.add(file);
            } else {
                dbFiles.addAll(Arrays.stream(files).collect(Collectors.toList()));
            }

            File file = dbFiles.get(dbFiles.size() - 1);
            this.fileNumber.set(dbFiles.size() - 1);

            MappedByteBuffer buffer = BackupUtil.fileMapped(file);
            this.lastModify.set(buffer);

            for (File dbFile : dbFiles) {
                BackupUtil.readFromDisk(this.engine, dbFile);
            }
        } catch (IOException e) {
            LOG.error(e.getMessage());
        } finally {
            this.writeLock.unlock();
        }
    }

    public void writeDataToDisk(CacheBuffer.Item item) {
        this.writeLock.lock();
        try {
            while (true) {
                MappedByteBuffer finalBuffer = lastModify.get();
                long p = finalBuffer.getLong();
                finalBuffer.position((int) Math.max(8, p));

                int written = 2 * 4 + item.key.length + item.value.length;
                if (finalBuffer.remaining() >= written) {
                    AtomicInteger l = new AtomicInteger(0);
                    write(finalBuffer, item.key);
                    write(finalBuffer, item.value);
                    finalBuffer.force();
                    finalBuffer.putLong(0, 8 + l.get());// 更新头的长度，也就是目前文件写到的位置
                    finalBuffer.force();
                    break;
                } else {
                    lastModify.set(BackupUtil.createNewFileAndMapped(fileNumber, this.dir));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            this.writeLock.unlock();
        }
    }

    public Object get(String key) {
        return this.engine.get(key);
    }

    public void save(List<LogEntry> logs) {
        for (LogEntry entry : logs) {
            this.set(String.valueOf(entry.getIndex()), entry);
        }
    }

    public void set(String key, Object value) {
        if (key == null || value == null) return;
        this.engine.set(key, value);
        byte[] kb = key.getBytes();
        byte[] vb = KryoUtil.asByteArray(value);
        this.writeDataToDisk(new CacheBuffer.Item(kb, vb));
    }

    public void remove(String key) {
        this.engine.remove(key);
    }
}
