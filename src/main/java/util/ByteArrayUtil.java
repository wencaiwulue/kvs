package util;

import java.nio.ByteBuffer;

/**
 * @author naison
 * @since 4/13/2020 20:49
 */
public class ByteArrayUtil {
    // optimize ?
    public static byte[] combine(byte[] k, byte[] v) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(k.length + v.length + 4 * 2);
        byteBuffer.putInt(k.length);
        byteBuffer.put(k);
        byteBuffer.putInt(v.length);
        byteBuffer.put(v);
        return byteBuffer.array();
    }
}
