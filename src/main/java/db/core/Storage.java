package db.core;

import util.FileUtil;
import util.KryoUtil;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Map;
import java.util.Spliterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static util.BackupUtil.write;

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

    @SuppressWarnings("ResultOfMethodCallIgnored")
    public Storage(Path dbDir, Lock lock, AtomicReference<MappedByteBuffer> lastModify) {
        this.writeLock = lock;
        this.lastModify = lastModify;
        this.dbSnapshotDir = Path.of(dbDir.toString(), "snapshot");
        this.map = new ConcurrentHashMap<>(/*1 << 30*/); // 这是hashMap的容量
        this.expireKeys = new PriorityBlockingQueue<>(11, ExpireKey::compareTo);
        File file = this.dbSnapshotDir.toFile();
        FileUtil.rm(file);
        try {
            file.mkdirs();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void snapshotRapidly() {
        this.writeLock.lock();
        try {
            AtomicInteger ai = new AtomicInteger(0);
            AtomicInteger l = new AtomicInteger(0);// 本次写入的量

            getMappedByteBuffer(ai, this.dbSnapshotDir, this.lastModify);
            Spliterator<Map.Entry<String, Object>> entrySpliterator = map.entrySet().spliterator().trySplit();
            System.out.println(ai.get());
            entrySpliterator.forEachRemaining(e -> {
                MappedByteBuffer finalBuffer = this.lastModify.get();
                byte[] key = e.getKey().getBytes();
                byte[] value = KryoUtil.asByteArray(e.getValue());

                if (finalBuffer.remaining() < 2 * 4 + key.length + value.length) {
                    finalBuffer.force();
                    finalBuffer.putLong(0, 8 + l.get());// 更新头的长度，也就是目前文件写到的位置
                    finalBuffer.force();
                    l.set(0);
                    this.lastModify.set(getMappedByteBuffer(ai, this.dbSnapshotDir, this.lastModify));
                } else {
                    write(finalBuffer, key, l);
                    write(finalBuffer, value, l);
                }
            });
        } finally {
            this.writeLock.unlock();
        }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    public static MappedByteBuffer getMappedByteBuffer(AtomicInteger ai, Path path, AtomicReference<MappedByteBuffer> lastModify) {
        try {
            File file = Path.of(path.toString(), ai.getAndIncrement() + ".db").toFile();
            file.createNewFile();
            RandomAccessFile raf = new RandomAccessFile(file, "rw");
            MappedByteBuffer mappedByteBuffer = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, Integer.MAX_VALUE);
            mappedByteBuffer.position(8);
            lastModify.set(mappedByteBuffer);
            return mappedByteBuffer;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static MappedByteBuffer getMappedByteBuffer(File file) {
        try {
            RandomAccessFile raf = new RandomAccessFile(file, "rw");
            MappedByteBuffer buffer = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, Integer.MAX_VALUE);
            long p = buffer.getLong(0);
            buffer.position((int) Math.max(8, p));
            return buffer;
        } catch (IOException e) {
            return null;
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
