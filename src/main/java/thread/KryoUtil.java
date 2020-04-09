package thread;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.util.concurrent.TimeUnit;

/**
 * 初步测试貌似这个真的快！！！
 * 但是缺点也很明显，需要加类锁，因为input和output是类属性，如果变成局部变量，性能就会比FST差。
 * 多线程情况下的FST和Kryo谁快还没测试，就目前来看，Kryo性能更优
 *
 * @author naison
 * @since 4/7/2020 22:45
 */
public class KryoUtil {
    private static final Kryo kryo = new Kryo();
    private static Input input = new Input(1024 * 10);// 10kb
    private static int outputSize = 10;
    private static Output output = new Output(1 << outputSize);

    public synchronized static byte[] asByteArray(Object o) {
        output.clear();
        int retry = 3;
        int t = 0;
        while (t++ < retry) {
            try {
                kryo.writeClassAndObject(output, o);
            } catch (KryoException e) {
                if (e.getMessage().contains("Buffer overflow")) {
                    output = new Output(1 << outputSize++);// 说明不够大，需要扩容
                    if (retry++ > 10) break;
                }
            }
        }
        return output.toBytes();
    }

    public synchronized static Object asObject(byte[] bytes) {
        input.setBuffer(bytes);
        return kryo.readClassAndObject(input);
    }

    public synchronized static Object asObject(byte[] bytes, int offset, int count) {
        input.setBuffer(bytes, offset, count);
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
