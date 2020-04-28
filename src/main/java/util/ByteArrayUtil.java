package util;

import org.nustaq.serialization.FSTObjectOutput;
import org.nustaq.serialization.util.FSTUtil;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author naison
 * @since 4/13/2020 20:49
 */
public class ByteArrayUtil {
    public static ByteBuffer write(Object object) {
        FSTObjectOutput objectOutput = util.FSTUtil.getConf().getObjectOutput();
        try {
            objectOutput.writeObject(object);
            int written = objectOutput.getCodec().getWritten();
            byte[] buffer = objectOutput.getBuffer();
            return ByteBuffer.wrap(buffer, 0, written);
        } catch (IOException e) {
            FSTUtil.rethrow(e);
        }
        return null;
    }

    public static byte[] intToByteArray(int value) {
        return new byte[]{
                (byte) (value >> 24),
                (byte) (value >> 16),
                (byte) (value >> 8),
                (byte) (value)};
    }

    public static int byteArrayToInt(byte[] bytes) {
        return ((bytes[0] & 0xFF) << 24) |
                ((bytes[1] & 0xFF) << 16) |
                ((bytes[2] & 0xFF) << 8) |
                ((bytes[3] & 0xFF));
    }


}
