package util.thread;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import util.ThreadUtil;

import java.util.Comparator;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * the reason why not use {@link java.util.concurrent.DelayQueue} is just because performance
 * DelayQueue needs to grow up and extend, it's waste too much time
 */
public class FakeDelayQueue {
    public static PriorityBlockingQueue<DelayTask> delayTasks =
            new PriorityBlockingQueue<>(1000 * 1000 * 100, Comparator.comparingLong(DelayTask::getFutureInMillis));

    static {
        Runnable r =
                () -> {
                    //noinspection InfiniteLoopStatement
                    while (true) {
                        if (!delayTasks.isEmpty()) {
                            DelayTask task = delayTasks.peek();
                            if (task != null && task.futureInMillis <= System.currentTimeMillis()) {
                                delayTasks.poll();
                                task.consume();
                            }
                        } else {
                            try {
                                // if this queue is empty, call take function will stock here, optimize performance
                                DelayTask task = delayTasks.take();
                                long i = task.futureInMillis - System.currentTimeMillis();
                                if (i > 0) {
                                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(i));
                                }
                                task.consume();
                            } catch (InterruptedException ex) {
                                ex.printStackTrace();
                            }
                        }
                    }
                };
        ThreadUtil.getThreadPool().submit(r);
    }

    public static DelayTask delay(DelayTask delayTask) {
        delayTasks.offer(delayTask);
        return delayTask;
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
    public static class DelayTask {
        private Runnable runnable;
        private long futureInMillis;

        public static DelayTask of(long futureInMillis, Runnable c) {
            return new DelayTask(c, futureInMillis);
        }

        private void consume() {
            ThreadUtil.getThreadPool().execute(this.runnable);
        }
    }
}
