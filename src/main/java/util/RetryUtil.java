package util;

import java.util.concurrent.*;

/**
 * @author naison
 * @since 4/17/2020 17:21
 */
public class RetryUtil {
    public static void retryIfException(int times, int offsetMills, Runnable r) {
        int i = 0;
        times = Math.max(1, times);// at lease one times
        while (i++ < times) {
            try {
                ThreadUtil.getThreadPool().execute(r);
                break;
            } catch (Exception ignored) {
                ThreadUtil.sleep(offsetMills);
            }
        }
    }

    public static Object retryIfException(int time, TimeUnit unit, Callable<?> c) {
        Future<?> submit = ThreadUtil.getThreadPool().submit(c);
        try {
            return submit.get(time, unit);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            e.printStackTrace();
            return null;
        }
    }
}
