package util;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.util.DefaultInstantiatorStrategy;
import com.esotericsoftware.kryo.util.Pool;
import org.objenesis.strategy.StdInstantiatorStrategy;

/**
 * 初步测试貌似这个真的快！！！
 * 但是缺点也很明显，需要加类锁，因为input和output是类属性，如果变成局部变量，性能就会比FST差。
 * 多线程情况下的FST和Kryo谁快还没测试，就目前来看，Kryo性能更优
 *
 * @author naison
 * @since 4/7/2020 22:45
 */
public class KryoUtil { // only use for writing data to disk
    private static final Kryo KRYO = new Kryo();
    private static final Input INPUT = new Input(1024 * 10);// 10KB
    private static int OUTPUT_SIZE = 10;
    private static Output OUTPUT = new Output(1 << OUTPUT_SIZE);
    private static final Pool<Kryo> KRYO_POOL;

    static {
        KRYO_POOL = new Pool<>(true, true, 100) {
            @Override
            protected Kryo create() {
                Kryo kryo = new Kryo();
                kryo.setReferences(true);
                kryo.setRegistrationRequired(false);
                ((DefaultInstantiatorStrategy) kryo.getInstantiatorStrategy()).setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());
                return kryo;
            }
        };
    }


    public static byte[] asByte(Object o) {
        Kryo obtain = KRYO_POOL.obtain();
        obtain.writeClassAndObject(OUTPUT, o);
        return OUTPUT.toBytes();
    }


    public synchronized static byte[] asByteArray(Object o) {
        OUTPUT.setPosition(0);
        int retry = 3;
        int t = 0;
        while (t++ < retry) {
            try {
                KRYO.writeClassAndObject(OUTPUT, o);
            } catch (KryoException e) {
                if (e.getMessage().contains("Buffer overflow")) {
                    OUTPUT = new Output(1 << OUTPUT_SIZE++);// 说明不够大，需要扩容
                    if (retry++ > 10) break;
                }
            }
        }
        return OUTPUT.toBytes();
    }

    public synchronized static Object asObject(byte[] bytes) {
        INPUT.setBuffer(bytes);
        return KRYO.readClassAndObject(INPUT);
    }

    public synchronized static Object asObject(byte[] bytes, int offset, int count) {
        INPUT.setBuffer(bytes, offset, count);
        return KRYO.readClassAndObject(INPUT);
    }
}
