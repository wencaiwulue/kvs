
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
    private static final ThreadPoolExecutor pool = new ThreadPoolExecutor(Util.MIN_THREAD_POOL_SIZE, Util.MAX_THREAD_POOL_SIZE, 60L, TimeUnit.SECONDS, new LinkedBlockingQueue<>());

    private static final ScheduledThreadPoolExecutor scheduledPool = new ScheduledThreadPoolExecutor(Util.MAX_SCHEDULED_THREAD_POOL_SIZE);

    public static ThreadPoolExecutor getThreadPool() {
        return pool;
    }

    public static ScheduledThreadPoolExecutor getScheduledThreadPool() {
        return scheduledPool;
    }
}
