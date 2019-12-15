package main.protocol;


import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;

/**
 * only support get and set
 * <p>
 * redis serialization protocol ??
 *
 * @author naison
 * @since 12/8/2019 14:01
 */
public class ProtocolParser {

    public static void main(String[] args) throws IOException {
        HashMap<Integer, Object> map = new HashMap<>(20);
        for (int i = 0; i < 20; i++) {
            map.put(i, i);
        }
        File file = new File("C:\\Users\\89570\\Desktop\\New Text Document.txt");
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
        FileChannel channel = randomAccessFile.getChannel();
        MappedByteBuffer map1 = channel.map(FileChannel.MapMode.READ_WRITE, 0, 2000);

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
        objectOutputStream.writeObject(map);
        byte[] bytes = byteArrayOutputStream.toByteArray();

        map1.put(bytes);
        map1.force();
    }

    public static void parse(String command) {

    }
}
