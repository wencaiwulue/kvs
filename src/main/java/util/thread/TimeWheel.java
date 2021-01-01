package util.thread;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import util.ThreadUtil;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.stream.IntStream;

public class TimeWheel {
    public int level;
    public int[] p;
    public long dial;
    public long step;
    public List<Task>[] tasks;

    @SuppressWarnings("unchecked")
    public TimeWheel(int level, long dial, long step) {
        this.level = level;
        this.p = new int[level];
        this.dial = dial;
        this.step = step;
        this.tasks = new LinkedList[(int) (this.dial * this.level)];
        for (int i = 0; i < this.tasks.length; i++) {
            this.tasks[i] = new LinkedList<>();
        }
        this.init();
    }

    private void init() {
        Runnable r =
                () -> {
                    this.p[0] += 1;
                    // get effect level
                    //1, just image the progress of clock: 23:59:59 --> 00:00:00
                    int e = 0;
                    for (int i = 0; i < this.level; i++) {
                        long quotient = this.p[i] / this.dial;
                        long remainder = this.p[i] % this.dial;
                        this.p[i] = (int) remainder;
                        if (quotient == 0) {
                            break;
                        } else {
                            this.p[i + 1] += quotient;
                            e = i + 1;
                            if (this.p[this.level - 1] == this.dial) {
                                e = this.level - 1;
                                // 23:59:59 --> 00:00:00
                                for (int j = 0; j < this.level; j++) {
                                    this.p[j] = 0;
                                }
                                break;
                            }
                        }
                    }

                    // 2, drop the task to the lower level
                    for (int i = e; i > 0; i--) {
                        long l = this.p[i] + this.dial * i;
                        List<Task> taskList = this.tasks[(int) l];
                        for (Task task : taskList) {
                            int finalI = i;
                            Runnable runnable = () -> {
                                int t = (int) task.unit.toMillis(task.period);
                                double remind = t % (Math.pow(this.dial, finalI));
                                long position = (long) ((remind + this.p[finalI - 1]) % this.dial);
                                this.tasks[(int) position].add(task);
                            };
                            ThreadUtil.getThreadPool().submit(runnable);
                        }
                        taskList.clear();
                    }

                    // 3, run the lowest level task
                    List<Task> taskList = this.tasks[this.p[0]];
                    for (Task task : taskList) {
                        ThreadUtil.getThreadPool().submit(() -> {
                            this.scheduleAtFixedRateInner(task);
                            task.runnable.run();
                        });
                    }
                    taskList.clear();
                };
        ThreadUtil.getThreadPool().submit(() -> {
            //noinspection InfiniteLoopStatement
            while (true) {
                // maybe using parkUntil ?? a abstract time
                LockSupport.parkNanos(1000 * 1000 * this.step);
                ThreadUtil.getThreadPool().submit(r);
            }
        });
    }

    public FakeDelayQueue.DelayTask scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
        Consumer<FakeDelayQueue.DelayTask> c = delayTask -> {
            this.scheduleAtFixedRateInner(delayTask.task);
            delayTask.task.runnable.run();
        };
        FakeDelayQueue.DelayTask delayTask = FakeDelayQueue.DelayTask
                .of(command, initialDelay, period, unit, t -> ThreadUtil.getThreadPool().submit(() -> c.accept(t)));
        FakeDelayQueue.delay(delayTask);
        return delayTask;
    }

    private Task scheduleAtFixedRateInner(Task task) {
        int l = (int) task.unit.toMillis(task.period);

        int level = (int) (Math.log(l) / Math.log(this.dial));
        int bucket = (int) (l / Math.pow(this.dial, level));

        // insert into relative bucket, not abstract bucket
        long l1 = level * this.dial + (bucket + this.p[level]) % this.dial;
        this.tasks[(int) l1].add(task);
        return task;
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
    public static class Task {
        public Runnable runnable;
        public long initialDelay;
        public long period;
        public TimeUnit unit;

        public static Task of(Runnable runnable, long initialDelay, long period, TimeUnit unit) {
            return new Task(runnable, initialDelay, period, unit);
        }
    }

    public static void main(String[] args) throws InterruptedException {
        AtomicLong ad = new AtomicLong(0);
        int i = 200 * 10000;
        int j = 1000 * 1000 * 5;
        AtomicLong start = new AtomicLong(System.nanoTime());
        Runnable r = () -> {
            long l = ad.incrementAndGet();
            if (l % j == 0) {
                long end = System.nanoTime();
                System.out.println(TimeUnit.NANOSECONDS.toSeconds(end - start.get()));
                start.set(end);
                System.out.println(l);
            }
        };
        Runnable empty = () -> {
        };
        TimeWheel timeWheel = new TimeWheel(4, 60, 1);
//        IntStream.range(0, i)
//                .forEach(e -> timeWheel.scheduleAtFixedRate(empty, 0, 123, TimeUnit.SECONDS));
//        IntStream.range(0, i)
//                .forEach(e -> timeWheel.scheduleAtFixedRate(empty, 0, 39, TimeUnit.SECONDS));
        IntStream.range(0, j)
                .forEach(e -> timeWheel.scheduleAtFixedRate(r, 5, 3, TimeUnit.SECONDS));

        Thread.sleep(Long.MAX_VALUE);
    }
}
