
package db.core;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import thread.FSTUtil;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author naison
 * @since 3/31/2020 20:56
 */
public class BackupUtil {
    private static final Logger log = LogManager.getLogger(BackupUtil.class);

    /*
     * 写入磁盘格式为
     * ---------------------------------------------------------------------
     *| 8byte | key length| key |value length | value | key length | ......
     * ---------------------------------------------------------------------
     * 固定的8个byte的头，用于存储实际使用大小
     * */
    public static void snapshotToDisk(ConcurrentHashMap<String, Object> map, RandomAccessFile raf) {
        if (map.isEmpty() || raf == null) return;

        try {
            long p = 8L;// 固定的8byte文件头
            try {
                p = raf.readLong();
            } catch (IOException ignored) {
            }

            FileChannel channel = raf.getChannel();
            MappedByteBuffer mapped = channel.map(FileChannel.MapMode.READ_WRITE, p, Integer.MAX_VALUE);
            AtomicInteger l = new AtomicInteger(0);// 本次写入的量
            for (Map.Entry<String, Object> next : map.entrySet()) {
                byte[] key = next.getKey().getBytes();
                write(mapped, key, l);
                byte[] value = FSTUtil.getConf().asByteArray(next.getValue());
                write(mapped, value, l);
                if (l.getAcquire() >= Integer.MAX_VALUE - 1024) {// 如果已经还剩1k byte空间的话，需要重新扩容
                    mapped = channel.map(FileChannel.MapMode.READ_WRITE, l.getAcquire(), Integer.MAX_VALUE);// 每次扩容都是2g, 这是也fileChannel的限制
                    // 目前主流的linux文件格式最大单体文件大小限制，ext3--16TB，ext4--1EB, 由于这是内存存储器，而RAM可能的容量是4T以下，所以暂时够用
                }
            }
            mapped.force();

            raf.seek(0);
            raf.writeLong(p + l.get());// 更新头的长度，也就是目前文件写到的位置
        } catch (IOException e) {
            e.printStackTrace();
            log.error("这里出问题了", e);
        }
    }


    public static void appendToDisk(ArrayDeque<byte[]> pipeline, int size, RandomAccessFile raf) {
        if (raf == null) return;

        try {
            long p = 8L;// 固定的8byte文件头
            try {
                p = raf.readLong();
            } catch (IOException ignored) {
            }
            FileChannel channel = raf.getChannel();
            MappedByteBuffer mapped = channel.map(FileChannel.MapMode.READ_WRITE, p, Integer.MAX_VALUE);
            int length = 0;// 本次写入的量
            for (int i = 0; i < size; i++) {
                byte[] bytes = pipeline.pollLast();
                if (bytes != null) {
                    mapped.put(bytes);
                    length += bytes.length;
                }
            }
            mapped.force();

            raf.seek(0);
            raf.writeLong(p + length);// 更新头的长度
        } catch (IOException e) {
            log.error(e);
        }
    }

    private static void write(MappedByteBuffer map, byte[] bytes, AtomicInteger l) {
        map.putInt(bytes.length);
        map.put(bytes);
        l.addAndGet(bytes.length + 4);
    }

    public static void readFromDisk(ConcurrentHashMap<String, Object> map, RandomAccessFile raf) {
        if (raf == null) return;

        try {
            long p = 8L;// 固定的8byte文件头
            try {
                p = raf.readLong();
            } catch (IOException ignored) {
            }
            FileChannel channel = raf.getChannel();
            MappedByteBuffer mapped = channel.map(FileChannel.MapMode.READ_WRITE, 8, p);// 跳过头位置
            while (mapped.hasRemaining()) {
                int kLen = mapped.getInt();
                byte[] kb = new byte[kLen];
                mapped.get(kb);
                String key = new String(kb);
                int valLen = mapped.getInt();
                byte[] vb = new byte[valLen];
                mapped.get(vb);
                Object value = FSTUtil.getConf().asObject(vb);
                map.put(key, value);
            }
        } catch (IOException e) {
            log.error(e);
        }
    }

    public static void main(String[] args) throws IOException {
        String path = "C:\\Users\\89570\\Documents\\test3.txt";
        File f = new File(path);
        if (!f.exists()) f.createNewFile();
        RandomAccessFile raf = new RandomAccessFile(f, "rw");
        int n =/*1 << 30*/ 1000 * 1000 * 10;
        ConcurrentHashMap<String, Object> map = new ConcurrentHashMap<>(n);

        long start = System.nanoTime();
        for (int i = 0; i < n; i++) {
            map.put(String.valueOf(i), i);
        }
        System.out.println("存入map花费时间：" + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) + "ms");
        start = System.nanoTime();
        snapshotToDisk(map, raf);
        System.out.println("写入磁盘花费时间：" + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) + "ms");
        start = System.nanoTime();
        readFromDisk(map, raf);
        System.out.println("写入内存花费时间：" + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) + "ms");
    }
}
