package thread;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * @author naison
 * @since 4/8/2020 15:17
 */
@SuppressWarnings("all")
public class Filter {
    public static void main(String[] args) throws IOException {
        BloomFilter<String> filter = BloomFilter.create(Funnels.stringFunnel(StandardCharsets.UTF_8), Integer.MAX_VALUE);
        for (long i = 0; i < 1000000; i++) {
            filter.put(String.valueOf(i));
        }
        String path = "C:\\Users\\89570\\Documents\\test5.txt";
        File file = new File(path);
        if (!file.exists()) file.createNewFile();

        FileOutputStream stream = new FileOutputStream(path);
        filter.writeTo(stream);
    }
}
