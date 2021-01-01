package util.thread;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import util.ThreadUtil;

import java.util.Comparator;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * the reason why not use {@link java.util.concurrent.DelayQueue} is just because performance
 * DelayQueue needs to grow up and extend, it's waste too much time
 */
public class FakeDelayQueue {
    public static PriorityBlockingQueue<DelayTask> delayTasks =
            new PriorityBlockingQueue<>(1000 * 1000 * 100, Comparator.comparingLong(DelayTask::getFuture));

    static {
        Runnable r =
                () -> {
                    //noinspection InfiniteLoopStatement
                    while (true) {
                        if (!delayTasks.isEmpty()) {
                            DelayTask peek = delayTasks.peek();
                            if (peek != null && peek.future <= System.nanoTime()) {
                                delayTasks.poll();
                                peek.c.accept(peek);
                            }
                        } else {
                            try {
                                DelayTask take = delayTasks.take();
                                if (take.future <= System.nanoTime()) {
                                    take.c.accept(take);
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
        public long future;
        // how to consume this delayTask if future is expired
        public Consumer<DelayTask> c;

        public static DelayTask of(Runnable runnable, long initialDelay, long period, TimeUnit unit, Consumer<DelayTask> c) {
            TimeWheel.Task task = TimeWheel.Task.of(runnable, initialDelay, period, unit);
            long future = System.nanoTime() + unit.toNanos(initialDelay);
            return new DelayTask(task, future, c);
        }
    }
}
