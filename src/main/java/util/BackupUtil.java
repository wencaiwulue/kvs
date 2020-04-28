
package util;

import com.esotericsoftware.kryo.KryoException;
import db.core.CacheBuffer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author naison
 * @since 3/31/2020 20:56
 */
public class BackupUtil {
    private static final Logger log = LogManager.getLogger(BackupUtil.class);

    /*
     * 写入磁盘格式为
     * -------------------------------------------------------------------------
     *| 8 byte | keyLength | key | valueLength | value | keyLength | key |......
     * -------------------------------------------------------------------------
     * 固定的8个byte的头，用于存储实际使用大小
     * */
    public static synchronized void snapshotToDisk(Map<String, Object> map, List<File> files, int maxFileSize) {
        if (map.isEmpty()) return;

        RandomAccessFile raf = getLastRAF(files);
        if (raf == null) return;

        long p = 8L;// 固定的8byte文件头
        try {
            raf.seek(0);
            p = raf.readLong();
        } catch (IOException e) {
            log.error(e);
        }

        FileChannel channel = raf.getChannel();
        MappedByteBuffer mapped = null;
        try {
            mapped = channel.map(FileChannel.MapMode.READ_WRITE, p, Integer.MAX_VALUE);
        } catch (IOException e) {
            e.printStackTrace();
            log.error(e);
        }
        if (mapped == null) return;

        AtomicInteger l = new AtomicInteger(0);// 本次写入的量
        for (Map.Entry<String, Object> next : map.entrySet()) {
            byte[] key = next.getKey().getBytes();
            write(mapped, key, l);
            byte[] value = KryoUtil.asByteArray(next.getValue());
            write(mapped, value, l);
            if (l.getAcquire() >= Integer.MAX_VALUE - 1024 * 10) {// 如果已经还剩1k byte空间的话，需要重新扩容
//                mapped = channel.map(FileChannel.MapMode.READ_WRITE, l.getAcquire(), Integer.MAX_VALUE);// 每次扩容都是2g, 这是也fileChannel的限制
                // 目前主流的linux文件格式最大单体文件大小限制，ext3--16TB，ext4--1EB, 由于这是内存存储器，而RAM可能的容量是4T以下，所以暂时够用
            }
        }
        mapped.force();

        try {
            raf.seek(0);
            raf.writeLong(p + l.get());// 更新头的长度，也就是目前文件写到的位置
        } catch (IOException e) {
            log.error(e);
        }
    }

    private static RandomAccessFile getLastRAF(List<File> files) {
        if (files.isEmpty()) return null;
        try {
            return new RandomAccessFile(files.get(files.size() - 1), "rw");
        } catch (FileNotFoundException e) {
            log.error(e);
            return null;
        }
    }


    public static synchronized void appendToDisk(CacheBuffer<CacheBuffer.Item> pipeline, int size, AtomicReference<MappedByteBuffer> reference, int maxFileSize) {
        if (pipeline.isEmpty()) return;
        MappedByteBuffer mapped = reference.get();
        if (mapped == null) return;
        int p = mapped.position();

        int length = 0;// 本次写入的量
        for (int i = 0; i < size; i++) {
            CacheBuffer.Item bytes = pipeline.pollLast();
            if (bytes != null) {
                try {
                    mapped.put(ByteArrayUtil.intToByteArray(bytes.key.length));
                    mapped.put(bytes.key);
                    mapped.put(ByteArrayUtil.intToByteArray(bytes.value.length));
                    mapped.put(bytes.value);
                    length += bytes.getSize();
                } catch (BufferOverflowException e) {
                    log.error("this is not impossible", e);
                }
            }
        }
        mapped.force();
        mapped.putLong(0, p + length);// 更新头的长度
        mapped.force();
    }

    public static void write(MappedByteBuffer map, byte[] bytes, AtomicInteger l) {
        try {
            map.putInt(bytes.length);
            map.put(bytes);
            l.addAndGet(bytes.length + 4);
        } catch (BufferOverflowException e) {
            System.out.println(l.get());
        }
    }

    public static synchronized void readFromDisk(Map<String, Object> map, File file) {
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(file, "rw");
        } catch (FileNotFoundException e) {
            log.error(e);
        }
        if (raf == null) return;

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
        for (int i = 0; i < t; i++) {// 裁剪的部位刚好是一个byte[]的中间，而不是一个与另一个byte[]之间的空隙
            long position = 8 + m * i - d;
            long size = (m * (i + 1) > p - 8) ? p - 8 : m * (i + 1);
            MappedByteBuffer mapped = null;// 跳过头位置
            try {
                mapped = channel.map(FileChannel.MapMode.READ_WRITE, position, size);
            } catch (IOException e) {
                e.printStackTrace();
                log.error(e);
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
    }

//    @SuppressWarnings("ResultOfMethodCallIgnored")
//    public static void main(String[] args) throws IOException {
//        String path = "C:\\Users\\89570\\Documents\\test3.txt";
//        File f = new File(path);
//        if (!f.exists()) f.createNewFile();
//        RandomAccessFile raf = new RandomAccessFile(f, "rw");
//        int n =/*1 << 30*/ 1000 * 1000 * 5;
//        ConcurrentHashMap<String, Object> map = new ConcurrentHashMap<>(n);
//        ConcurrentHashMap<String, Object> map1 = new ConcurrentHashMap<>(n);
//
//        long start = System.nanoTime();
//        for (int i = 0; i < n; i++) {
//            map.put(String.valueOf(i), i);
//        }
//        System.out.println("存入map花费时间：" + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) + "ms");
//        start = System.nanoTime();
//        snapshotToDisk(map, raf, Integer.MAX_VALUE);
//        System.out.println("写入磁盘花费时间：" + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) + "ms");
//        start = System.nanoTime();
//        readFromDisk(map1, raf);
//        System.out.println("写入内存花费时间：" + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) + "ms");
//        int m = 0;
//        for (int i = 0; i < n; i++) {
//            if (!map1.containsKey(String.valueOf(i)) || (int) map1.get(String.valueOf(i)) != i) {
//                m++;
//            }
//        }
//        System.out.println("不匹配的数量为：" + m);
//    }
}
