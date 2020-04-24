package util;

import rpc.model.requestresponse.Response;

import java.util.concurrent.*;
import java.util.function.Function;

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

    public static Response retryWithResultChecker(Callable<Response> c, Function<Response, Boolean> checker, int retry) {
        int i = Math.max(1, retry);
        int t = 0;
        while (t++ < i) {
            try {
                Response call = c.call();
                Boolean apply = checker.apply(call);
                if (apply) {
                    return call;
                }
            } catch (Exception ignored) {
            }
        }
        return null;
    }
}
