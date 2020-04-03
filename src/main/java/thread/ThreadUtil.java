package thread;

import java.util.concurrent.*;

/**
 * @author naison
 * @since 3/25/2020 17:21
 */
public class ThreadUtil {
    private static final ExecutorService pool = new ThreadPoolExecutor(10, 20, 1, TimeUnit.MINUTES, new LinkedBlockingQueue<>());
    private static final ScheduledThreadPoolExecutor schedulePool = new ScheduledThreadPoolExecutor(5);

    public static ExecutorService getPool() {
        return pool;
    }

    public static ScheduledThreadPoolExecutor getSchedulePool() {
        return schedulePool;
    }
}
