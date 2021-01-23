import org.junit.jupiter.api.Test;
import util.FSTUtil;
import util.KryoUtil;

import java.util.concurrent.TimeUnit;

public class JsonTest {
    @Test
    public void test() {
        long start = System.nanoTime();
        int n = 10000000;
        for (int i = 0; i < n; i++) {
            String s = String.valueOf(i);
            byte[] bytes = KryoUtil.asByteArray(s);
            String o = (String) KryoUtil.asObject(bytes);
        }

        System.out.println(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
        start = System.nanoTime();
        for (int i = 0; i < n; i++) {
            String s = String.valueOf(i);
            byte[] bytes = FSTUtil.getBinaryConf().asByteArray(s);
            String o = (String) FSTUtil.getBinaryConf().asObject(bytes);
        }
        System.out.println(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
    }
}
