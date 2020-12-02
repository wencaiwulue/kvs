import org.junit.jupiter.api.Test;
import util.BackupUtil;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class FileTest {
    @Test
    public void test() throws IOException {
        String path = "C:\\Users\\89570\\Documents\\test3.txt";
        File file = Path.of(path).toFile();
        if (!file.exists()) {
            boolean a = file.createNewFile();
        }
        MappedByteBuffer mappedByteBuffer = BackupUtil.fileMapped(file);
        int n =/*1 << 30*/ 1000 * 1000 * 5;
        ConcurrentHashMap<String, Object> map = new ConcurrentHashMap<>(n);
        ConcurrentHashMap<String, Object> map1 = new ConcurrentHashMap<>(n);

        long start = System.nanoTime();
        for (int i = 0; i < n; i++) {
            map.put(String.valueOf(i), i);
        }
        System.out.println("存入map花费时间：" + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) + "ms");
        start = System.nanoTime();
        BackupUtil.snapshotToDisk(map, Path.of(path).getParent(), new AtomicReference<>(mappedByteBuffer), new AtomicInteger(0));
        System.out.println("写入磁盘花费时间：" + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) + "ms");
        start = System.nanoTime();
        BackupUtil.readFromDisk(map1, file);
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
