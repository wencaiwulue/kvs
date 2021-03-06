
package util;

import com.esotericsoftware.kryo.KryoException;
import db.core.pojo.CacheBuffer;
import db.config.Config;
import db.core.storage.StorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.LogEntry;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Spliterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author naison
 * @since 3/31/2020 20:56
 */
public class BackupUtil {
    private static final Logger LOG = LoggerFactory.getLogger(BackupUtil.class);

    /*
     * 写入磁盘格式为
     * -------------------------------------------------------------------------
     *| 8 byte | keyLength | key | valueLength | value | keyLength | key |......
     * -------------------------------------------------------------------------
     * 固定的8个byte的头，用于存储实际使用大小
     * */
    public static synchronized void snapshotToDisk(Map<String, Object> map, Path dir, AtomicReference<MappedByteBuffer> lastModify, AtomicInteger fileNumber) {
        if (map.isEmpty()) return;

        AtomicInteger temp = new AtomicInteger(0);
        lastModify.set(createNewFileAndMapped(temp, dir));

        AtomicInteger l = new AtomicInteger(0);// 本次写入的量
        Spliterator<Map.Entry<String, Object>> entrySpliterator = map.entrySet().spliterator().trySplit();
        entrySpliterator.forEachRemaining(e -> {
            while (true) {
                MappedByteBuffer finalBuffer = lastModify.get();
                byte[] key = e.getKey().getBytes();
                byte[] value = KryoUtil.asByteArray(e.getValue());

                if (finalBuffer.remaining() >= 2 * 4 + key.length + value.length) {
                    write(finalBuffer, key);
                    write(finalBuffer, value);
                    l.addAndGet(4 * 2);
                    break;
                } else {
                    finalBuffer.force();
                    finalBuffer.putLong(0, 8 + l.get());// 更新头的长度，也就是目前文件写到的位置
                    finalBuffer.force();
                    l.set(0);
                    lastModify.set(createNewFileAndMapped(fileNumber, dir));
                }
            }
        });
        fileNumber.set(temp.get());
    }


    public static synchronized void snapshotToDisk(StorageEngine map, Path dir, AtomicReference<MappedByteBuffer> lastModify, AtomicInteger fileNumber) {
        Iterator<Object> iterator = map.iterator();
        while (!iterator.hasNext()) return;

        AtomicInteger temp = new AtomicInteger(0);
        lastModify.set(createNewFileAndMapped(temp, dir));

        AtomicInteger l = new AtomicInteger(0);// 本次写入的量
        map.iterator().forEachRemaining(e -> {
            Map.Entry<String, Object> ee = (Map.Entry<String, Object>) e;
            while (true) {
                MappedByteBuffer finalBuffer = lastModify.get();
                byte[] key = ee.getKey().getBytes();
                byte[] value = KryoUtil.asByteArray(ee.getValue());

                if (finalBuffer.remaining() >= 2 * 4 + key.length + value.length) {
                    write(finalBuffer, key);
                    write(finalBuffer, value);
                    l.addAndGet(4 * 2);
                    break;
                } else {
                    finalBuffer.force();
                    finalBuffer.putLong(0, 8 + l.get());// 更新头的长度，也就是目前文件写到的位置
                    finalBuffer.force();
                    l.set(0);
                    lastModify.set(createNewFileAndMapped(fileNumber, dir));
                }
            }
        });
        fileNumber.set(temp.get());
    }

    private static RandomAccessFile toRAF(File file) {
        if (file == null) return null;
        try {
            return new RandomAccessFile(file, "rw");
        } catch (FileNotFoundException e) {
            LOG.error(e.getMessage());
            return null;
        }
    }

    public static synchronized void appendToDisk(CacheBuffer<CacheBuffer.Item> pipeline, int size, Path dir, AtomicReference<MappedByteBuffer> lastModify, AtomicInteger fileNumber) {
        if (pipeline.isEmpty()) return;
        MappedByteBuffer mapped = lastModify.get();
        if (mapped == null) return;
        int p = mapped.position();

        int length = 0;// 本次写入的量
        for (int i = 0; i < size; i++) {
            CacheBuffer.Item bytes = pipeline.peek();
            if (bytes != null) {
                while (true) {
                    MappedByteBuffer byteBuffer = lastModify.get();
                    if (byteBuffer.remaining() >= 2 * 4 + bytes.key.length + bytes.value.length) {
                        byteBuffer.putInt(bytes.key.length);
                        byteBuffer.put(bytes.key);
                        byteBuffer.putInt(bytes.value.length);
                        byteBuffer.put(bytes.value);
                        length += bytes.getSize();
                        pipeline.poll();
                        break;
                    } else {
                        byteBuffer.force();
                        byteBuffer.putLong(0, p + length);// 更新头的长度
                        byteBuffer.force();
                        lastModify.set(createNewFileAndMapped(fileNumber, dir));
                    }
                }
            }
        }
        mapped.force();
        mapped.putLong(0, p + length);// 更新头的长度
        mapped.force();
    }

    public static void write(MappedByteBuffer map, byte[] bytes) {
        try {
            map.putInt(bytes.length);
            map.put(bytes);
        } catch (BufferOverflowException e) {
            LOG.error(e.getMessage());
            e.printStackTrace();
        }
    }

    public static synchronized void readFromDisk(StorageEngine engine, File file) {
        RandomAccessFile raf = toRAF(file);
        if (raf == null) return;

        long p = 8L;// 固定的8byte文件头
        try {
            raf.seek(0);
            p = raf.readLong();
        } catch (IOException ignored) {
        }
        FileChannel channel = raf.getChannel();
        p = Math.max(8, p);

        int m = Config.MAX_FILE_SIZE >> 6;// 裁剪的大小

        int t = (int) Math.ceil((double) (p - 8) / m);// 经测试貌似有点儿慢

        long len = 0;
        long d = 0;

        byte[] bytes = new byte[1024];
        for (int i = 0; i < t; i++) {// 裁剪的部位刚好是一个byte[]的中间，而不是一个与另一个byte[]之间的空隙
            long position = 8 + m * i - d;
            long size = (m * (i + 1) > p - 8) ? p - 8 : m * (i + 1);
            MappedByteBuffer mapped = null;// 跳过头位置
            try {
                mapped = channel.map(FileChannel.MapMode.READ_WRITE, position, size);
            } catch (IOException e) {
                e.printStackTrace();
                LOG.error(e.getMessage());
            }
            if (mapped == null) return;

            int a = 0, b = 0, c = 0;
            d = 0;
            len = 0;
            while (mapped.hasRemaining() /*&& mapped.remaining() >= 1024 * 10*/) {
                len += a + b + c * 2;
                try {
                    int kLen = mapped.getInt();
                    if (kLen > bytes.length) {
                        bytes = new byte[kLen];
                    }
                    mapped.get(bytes, 0, kLen);
                    String key = new String(bytes, 0, kLen);
                    int valLen = mapped.getInt();
                    if (valLen > bytes.length) {
                        bytes = new byte[valLen];
                    }
                    mapped.get(bytes, 0, valLen);
                    Object value = KryoUtil.asObject(bytes, 0, valLen);
                    engine.set(key, value);
                    a = kLen;
                    b = valLen;
                    c = 4;
                } catch (BufferUnderflowException | IndexOutOfBoundsException | KryoException e) {
                    d = size - len;
                    break;
                }
            }
        }
    }

    public static synchronized void readFromDisk(Collection<Object> collection, File file) {
        RandomAccessFile raf = toRAF(file);
        if (raf == null) return;

        long p = 8L;// 固定的8byte文件头
        try {
            raf.seek(0);
            p = raf.readLong();
        } catch (IOException ignored) {
        }
        p = Math.max(8, p);
        FileChannel channel = raf.getChannel();

        int m = Config.MAX_FILE_SIZE >> 6;// 裁剪的大小

        int t = (int) Math.ceil((double) (p - 8) / m);// 经测试貌似有点儿慢

        long len = 0;
        long d = 0;

        byte[] bytes = new byte[1024];
        for (int i = 0; i < t; i++) {// 裁剪的部位刚好是一个byte[]的中间，而不是一个与另一个byte[]之间的空隙
            long position = 8 + m * i - d;
            long size = (m * (i + 1) > p - 8) ? p - 8 : m * (i + 1);
            MappedByteBuffer mapped = null;// 跳过头位置
            try {
                mapped = channel.map(FileChannel.MapMode.READ_WRITE, position, size);
            } catch (IOException e) {
                e.printStackTrace();
                LOG.error(e.getMessage());
            }
            if (mapped == null) return;

            int a = 0, b = 0, c = 0;
            d = 0;
            len = 0;
            while (mapped.hasRemaining() /*&& mapped.remaining() >= 1024 * 10*/) {
                len += a + b + c * 2;
                try {
                    int kLen = mapped.getInt();
                    mapped.position(mapped.position() + kLen);// 跳过key
                    int valLen = mapped.getInt();
                    if (valLen > bytes.length) {
                        bytes = new byte[valLen];
                    }
                    mapped.get(bytes, 0, valLen);
                    Object value = KryoUtil.asObject(bytes, 0, valLen);
                    collection.add(value);
                    a = kLen;
                    b = valLen;
                    c = 4;
                } catch (BufferUnderflowException | IndexOutOfBoundsException | KryoException e) {
                    d = size - len;
                    break;
                }
            }
        }
    }

    public static synchronized void readFromDisk(Map<String, Object> collection, File file) {
        RandomAccessFile raf = toRAF(file);
        if (raf == null) return;

        long p = 8L;// 固定的8byte文件头
        try {
            raf.seek(0);
            p = raf.readLong();
        } catch (IOException ignored) {
        }
        p = Math.max(8, p);
        FileChannel channel = raf.getChannel();

        int m = Config.MAX_FILE_SIZE >> 6;// 裁剪的大小

        int t = (int) Math.ceil((double) (p - 8) / m);// 经测试貌似有点儿慢

        long len = 0;
        long d = 0;

        byte[] bytes = new byte[1024];
        for (int i = 0; i < t; i++) {// 裁剪的部位刚好是一个byte[]的中间，而不是一个与另一个byte[]之间的空隙
            long position = 8 + m * i - d;
            long size = (m * (i + 1) > p - 8) ? p - 8 : m * (i + 1);
            MappedByteBuffer mapped = null;// 跳过头位置
            try {
                mapped = channel.map(FileChannel.MapMode.READ_WRITE, position, size);
            } catch (IOException e) {
                e.printStackTrace();
                LOG.error(e.getMessage());
            }
            if (mapped == null) return;

            int a = 0, b = 0, c = 0;
            d = 0;
            len = 0;
            while (mapped.hasRemaining() /*&& mapped.remaining() >= 1024 * 10*/) {
                len += a + b + c * 2;
                try {
                    int kLen = mapped.getInt();
                    mapped.position(mapped.position() + kLen);// 跳过key
                    int valLen = mapped.getInt();
                    if (valLen > bytes.length) {
                        bytes = new byte[valLen];
                    }
                    mapped.get(bytes, 0, valLen);
                    Object value = KryoUtil.asObject(bytes, 0, valLen);
                    LogEntry log = (LogEntry) value;
                    collection.put(log.getKey(), log.getValue());
                    a = kLen;
                    b = valLen;
                    c = 4;
                } catch (BufferUnderflowException | IndexOutOfBoundsException | KryoException e) {
                    d = size - len;
                    break;
                }
            }
        }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    public static MappedByteBuffer createNewFileAndMapped(AtomicInteger fileNumber, Path dir) {
        try {
            File file = dir.toFile();
            if (!file.exists()) {
                file.mkdirs();
            }
            file = Path.of(dir.toString(), fileNumber.getAndIncrement() + ".db").toFile();
            file.createNewFile();
            return fileMapped(file);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static MappedByteBuffer fileMapped(File file) {
        try {
            RandomAccessFile raf = new RandomAccessFile(file, "rw");
            MappedByteBuffer buffer = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, Config.MAX_FILE_SIZE);
            long p = buffer.getLong(0);
            buffer.position((int) Math.max(8, p));
            return buffer;
        } catch (IOException e) {
            return null;
        }
    }


}
