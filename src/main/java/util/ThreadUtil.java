
package util;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author naison
 * @since 3/25/2020 17:21
 */
public class ThreadUtil {
    private static final ThreadPoolExecutor POOL =
            new ThreadPoolExecutor(Util.MIN_THREAD_POOL_SIZE, Util.MAX_THREAD_POOL_SIZE, 60L, TimeUnit.SECONDS, new LinkedBlockingQueue<>(), new KVSThreadFactory(), (r, executor) -> System.out.println("这里有个任务死掉了：" + r));

    private static final ScheduledThreadPoolExecutor SCHEDULED_POOL = new ScheduledThreadPoolExecutor(Util.MAX_SCHEDULED_THREAD_POOL_SIZE);

    public static ThreadPoolExecutor getThreadPool() {
        return POOL;
    }

    public static ScheduledThreadPoolExecutor getScheduledThreadPool() {
        return SCHEDULED_POOL;
    }

    public static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static class KVSThreadFactory implements ThreadFactory {
        private static final AtomicInteger poolNumber = new AtomicInteger(1);
        private final ThreadGroup group;
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;

        public KVSThreadFactory() {
            SecurityManager s = System.getSecurityManager();
            group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
            namePrefix = "kvs-pool-" + poolNumber.getAndIncrement() + "-thread-";
        }

        public Thread newThread(Runnable r) {
            Thread t = new Thread(group, r, namePrefix + threadNumber.getAndIncrement(), 0);
            if (t.isDaemon())
                t.setDaemon(false);
            if (t.getPriority() != Thread.NORM_PRIORITY)
                t.setPriority(Thread.NORM_PRIORITY);
            return t;
        }
    }
}
