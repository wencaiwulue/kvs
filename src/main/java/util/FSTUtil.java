package util;

import org.nustaq.serialization.FSTConfiguration;

import java.nio.ByteBuffer;

/**
 * @author naison
 * @since 3/25/2020 17:25
 */
public class FSTUtil { // use for communication serialization

    private static final FSTConfiguration conf = FSTConfiguration.createDefaultConfiguration();

    public static FSTConfiguration getConf() {
        return conf;
    }

    public static ByteBuffer asArrayWithLength(Object object) {
        byte[] bytes = FSTUtil.getConf().asByteArray(object);
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(4 + bytes.length);
        byteBuffer.putInt(bytes.length);
        byteBuffer.put(bytes);
        byteBuffer.rewind(); // 这一步真是搞死人啊。原来ByteBuffer.warp()的时候，position是0。。
        return byteBuffer;
    }
}
