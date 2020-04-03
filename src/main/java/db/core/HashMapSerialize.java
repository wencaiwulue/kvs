
package db.core;

import org.nustaq.serialization.FSTConfiguration;
import thread.FSTUtil;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystemException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author naison
 * @since 3/31/2020 20:56
 */
public class HashMapSerialize {
    public static void main(String[] args) throws IOException {
        String path = "C:\\Users\\89570\\Documents\\test3.txt";
        int n = 10000000;
        ConcurrentHashMap<String, Object> map = new ConcurrentHashMap<>(n);
        for (int i = 0; i < n; i++) {
            map.put(String.valueOf(i), i);
        }
        long start = System.nanoTime();
        writeToDisk(map, path);
        System.out.println("写入磁盘花费时间：" + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) + "ms");
        start = System.nanoTime();
        readFromDisk(map, path);
        System.out.println("写入内存花费时间：" + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) + "ms");
    }

    /*
     * 写入磁盘格式为
     * ---------------------------------------------------------------------
     *| 8byte | key length| key |value length | value | key length | ......
     * ---------------------------------------------------------------------
     * 固定的8个byte的头，用于存储实际使用大小
     * */
    private static void writeToDisk(ConcurrentHashMap<String, Object> map, String path) throws IOException {
        File file = new File(path);

        long p;
        RandomAccessFile raf;
        if (file.exists()) {
            raf = new RandomAccessFile(file, "rw");
            p = raf.readLong();
        } else {
            boolean newFile = file.createNewFile();
            if (!newFile) {
                throw new FileSystemException("create file failed!!!");
            }
            raf = new RandomAccessFile(file, "rw");
            p = 8L;// 固定的头，标识现在已经写到的位置，单位byte
        }

        FileChannel channel = raf.getChannel();
        MappedByteBuffer mappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, p, Integer.MAX_VALUE);
        AtomicInteger length = new AtomicInteger(0);// 本次写入的量
        for (Map.Entry<String, Object> next : map.entrySet()) {
            byte[] key = next.getKey().getBytes();
            write(mappedByteBuffer, key, length);
            byte[] value = FSTUtil.getConf().asByteArray(next.getValue());
            write(mappedByteBuffer, value, length);
        }
        mappedByteBuffer.force();

        raf.seek(0);
        raf.writeLong(p + length.get());// 更新头的长度
        raf.close();
    }

    private static void write(MappedByteBuffer map, byte[] bytes, AtomicInteger l) {
        map.putInt(bytes.length);
        map.put(bytes);
        l.addAndGet(bytes.length + 4);
    }

    private static void readFromDisk(ConcurrentHashMap<String, Object> map, String path) throws IOException {
        FSTConfiguration conf = FSTConfiguration.createDefaultConfiguration();
        File file = new File(path);

        long p;
        RandomAccessFile randomAccessFile = null;
        if (file.exists()) {
            randomAccessFile = new RandomAccessFile(file, "rw");
            p = randomAccessFile.readLong();
        } else {
            p = -1;
        }

        if (p < 0) {
            throw new FileNotFoundException("文件没找到");
        }

        FileChannel channel = randomAccessFile.getChannel();
        MappedByteBuffer mappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, p);
        mappedByteBuffer.position(8);// 跳过头位置
        while (mappedByteBuffer.hasRemaining()) {
            int keyLength = mappedByteBuffer.getInt();
            byte[] chars = new byte[keyLength];
            mappedByteBuffer.get(chars);
            String key = new String(chars);
            int valueLength = mappedByteBuffer.getInt();
            byte[] vb = new byte[valueLength];
            mappedByteBuffer.get(vb);
            Object value = conf.asObject(vb);
            map.put(key, value);
        }
        channel.close();
        randomAccessFile.close();
    }
}
