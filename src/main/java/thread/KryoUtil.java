package thread;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.util.concurrent.TimeUnit;

/**
 * 初步测试貌似这个真的快！！！
 *
 * @author naison
 * @since 4/7/2020 22:45
 */
public class KryoUtil {
    private static final Kryo kryo = new Kryo();
    private static final Input input = new Input(2048 * 10);
    private static final Output output = new Output(2048 * 10);

    public static byte[] asByteArray(Object o) {
        output.clear();
        kryo.writeClassAndObject(output, o);
        return output.getBuffer();
    }

    public static Object asObject(byte[] bytes) {
        input.setBuffer(bytes);
        return kryo.readClassAndObject(input);
    }

    public static void main(String[] args) {
        long start = System.nanoTime();
        int n = 10000000;
        for (int i = 0; i < n; i++) {
            String s = String.valueOf(i);
            byte[] bytes = KryoUtil.asByteArray(s);
            String o = (String) asObject(bytes);
        }

        System.out.println(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
        start = System.nanoTime();
        for (int i = 0; i < n; i++) {
            String s = String.valueOf(i);
            byte[] bytes = FSTUtil.getConf().asByteArray(s);
            String o = (String) FSTUtil.getConf().asObject(bytes);
        }
        System.out.println(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
    }

}
