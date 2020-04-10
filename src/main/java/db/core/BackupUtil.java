
package db.core;

import com.esotericsoftware.kryo.KryoException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import thread.KryoUtil;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
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
    public static synchronized void snapshotToDisk(ConcurrentHashMap<String, Object> map, RandomAccessFile raf) {
        if (map.isEmpty() || raf == null) return;

        try {
            long p = 8L;// 固定的8byte文件头
            try {
                raf.seek(0);
                p = raf.readLong();
            } catch (IOException ignored) {
            }

            FileChannel channel = raf.getChannel();
            MappedByteBuffer mapped = channel.map(FileChannel.MapMode.READ_WRITE, p, Integer.MAX_VALUE);
            AtomicInteger l = new AtomicInteger(0);// 本次写入的量
            for (Map.Entry<String, Object> next : map.entrySet()) {
                byte[] key = next.getKey().getBytes();
                write(mapped, key, l);
                byte[] value = KryoUtil.asByteArray(next.getValue());
                write(mapped, value, l);
                if (l.getAcquire() >= Integer.MAX_VALUE - 1024 * 10) {// 如果已经还剩1k byte空间的话，需要重新扩容
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


    public static synchronized void appendToDisk(ArrayDeque<byte[]> pipeline, int size, RandomAccessFile raf) {
        if (raf == null) return;

        try {
            long p = 8L;// 固定的8byte文件头
            try {
                raf.seek(0);
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
        try {
            map.putInt(bytes.length);
            map.put(bytes);
            l.addAndGet(bytes.length + 4);
        } catch (BufferOverflowException e) {
            System.out.println(l.get());
        }
    }

    public static synchronized void readFromDisk(ConcurrentHashMap<String, Object> map, RandomAccessFile raf) {
        if (raf == null) return;

        try {
            long p = 8L;// 固定的8byte文件头
            try {
                raf.seek(0);
                p = raf.readLong();
            } catch (IOException ignored) {
            }
            FileChannel channel = raf.getChannel();

            int m = Integer.MAX_VALUE >> 6;// 裁剪的大小

            int t = (int) Math.ceil((double) (p - 8) / m);// 经测试貌似有点儿慢

            long len = 0;
            long d = 0;

            byte[] bytes = new byte[1024];
            for (int i = 0; i < t; i++) {// todo 裁剪的部位刚好是一个byte[]的中间，而不是一个与另一个byte[]之间的空隙
                long position = 8 + m * i - d;
                long size = (m * (i + 1) > p - 8) ? p - 8 : m * (i + 1);
                MappedByteBuffer mapped = channel.map(FileChannel.MapMode.READ_WRITE, position, size);// 跳过头位置
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
                        map.put(key, value);
                        a = kLen;
                        b = valLen;
                        c = 4;
                    } catch (BufferUnderflowException | IndexOutOfBoundsException | KryoException e) {
                        d = size - len;
                        break;
                    }
                }
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
        int n =/*1 << 30*/ 1000 * 1000 * 5;
        ConcurrentHashMap<String, Object> map = new ConcurrentHashMap<>(n);
        ConcurrentHashMap<String, Object> map1 = new ConcurrentHashMap<>(n);

        long start = System.nanoTime();
        for (int i = 0; i < n; i++) {
            map.put(String.valueOf(i), i);
        }
        System.out.println("存入map花费时间：" + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) + "ms");
        start = System.nanoTime();
        snapshotToDisk(map, raf);
        System.out.println("写入磁盘花费时间：" + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) + "ms");
        start = System.nanoTime();
        readFromDisk(map1, raf);
        System.out.println("写入内存花费时间：" + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) + "ms");
        int m = 0;
        for (int i = 0; i < n; i++) {
            if (!map1.containsKey(String.valueOf(i)) || (int) map1.get(String.valueOf(i)) != i) {
                m++;
            }
        }
        System.out.println("不匹配的数量为：" + m);
    }
}
