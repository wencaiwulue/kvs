package db.core;

import util.BackupUtil;
import util.FileUtil;

import java.io.File;
import java.nio.MappedByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author naison
 * @since 4/28/2020 18:03
 */
public class Storage {
    private final Path dbSnapshotDir;
    public final ConcurrentHashMap<String, Object> map;// try RockDB, LevelDB or B-tree
    public final PriorityBlockingQueue<ExpireKey> expireKeys; // java.util.PriorityQueue ??
    private final Lock writeLock;
    private final AtomicReference<MappedByteBuffer> lastModify;

    public Storage(Path dbDir, Lock lock, AtomicReference<MappedByteBuffer> lastModify) {
        this.writeLock = lock;
        this.lastModify = lastModify;
        this.dbSnapshotDir = Path.of(dbDir.toString(), "snapshot");
        this.map = new ConcurrentHashMap<>(/*1 << 30*/); // 这是hashMap的容量
        this.expireKeys = new PriorityBlockingQueue<>(11, ExpireKey::compareTo);
        File file = this.dbSnapshotDir.toFile();
        FileUtil.emptyFolder(file);
    }

    public void snapshotRapidly() {
        this.writeLock.lock();
        try {
            BackupUtil.snapshotToDisk(this.map, this.dbSnapshotDir, this.lastModify);
        } finally {
            this.writeLock.unlock();
        }
    }

    public static void main(String[] args) {
        Path of = Path.of("C:\\Users\\89570\\Documents");
        Storage storage = new Storage(of, new ReentrantLock(), new AtomicReference<>());
        for (int i = 0; i < 1000000; i++) {
            storage.map.put(String.valueOf(i), i);
        }
        storage.snapshotRapidly();
    }
}
