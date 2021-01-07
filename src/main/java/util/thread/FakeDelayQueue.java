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
import java.util.function.Consumer;

/**
 * the reason why not use {@link java.util.concurrent.DelayQueue} is just because performance
 * DelayQueue needs to grow up and extend, it's waste too much time
 */
public class FakeDelayQueue {
    public static PriorityBlockingQueue<DelayTask> delayTasks =
            new PriorityBlockingQueue<>(1000 * 1000 * 100, Comparator.comparingLong(DelayTask::getFutureInNanos));

    static {
        Runnable r =
                () -> {
                    //noinspection InfiniteLoopStatement
                    while (true) {
                        if (!delayTasks.isEmpty()) {
                            DelayTask peek = delayTasks.peek();
                            if (peek != null && peek.futureInNanos <= System.nanoTime()) {
                                delayTasks.poll();
                                peek.c.accept(peek);
                            }
                        } else {
                            try {
                                DelayTask take = delayTasks.take();
                                if (take.futureInNanos <= System.nanoTime()) {
                                    take.c.accept(take);
                                } else if (TimeUnit.NANOSECONDS.toSeconds(take.futureInNanos - System.nanoTime()) > 1) {
                                    LockSupport.parkNanos(TimeUnit.NANOSECONDS.toSeconds(take.futureInNanos - System.nanoTime()) - 1);
                                    delayTasks.offer(take);
                                } else {
                                    delayTasks.offer(take);
                                }
                            } catch (InterruptedException exception) {
                                exception.printStackTrace();
                            }
                        }
                    }
                };
        ThreadUtil.getThreadPool().submit(r);
    }

    public static DelayTask delay(DelayTask task) {
        delayTasks.offer(task);
        return task;
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
    public static class DelayTask {
        public TimeWheel.Task task;
        public long futureInNanos;
        // how to consume this delayTask if future is expired
        public Consumer<DelayTask> c;

        public static DelayTask of(TimeWheel.Task task, Consumer<DelayTask> c) {
            long future = System.nanoTime() + task.unit.toNanos(task.initialDelay);
            return new DelayTask(task, future, c);
        }
    }
}
