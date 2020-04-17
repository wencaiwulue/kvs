package util;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author naison
 * @since 4/17/2020 15:31
 */
public class IdUtil {
    private static final AtomicInteger ai = new AtomicInteger(0);

    public static int get() {
        return ai.getAndIncrement();
    }
}
