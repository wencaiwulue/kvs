
package util;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author naison
 * @since 3/25/2020 17:21
 */
public class ThreadUtil {
    private static final ThreadPoolExecutor pool;
    private static final ScheduledThreadPoolExecutor schedulePool;

    static {
        int n = Math.max(Runtime.getRuntime().availableProcessors(), 3);// minimal thread amount is 3
        pool = new ThreadPoolExecutor(n, n * 2, 60L, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
        schedulePool = new ScheduledThreadPoolExecutor(1);
    }

    public static ThreadPoolExecutor getPool() {
        return pool;
    }

    public static ScheduledThreadPoolExecutor getSchedulePool() {
        return schedulePool;
    }
}
