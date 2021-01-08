package util.thread;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import util.ThreadUtil;

import java.util.Arrays;
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
    public long[] dial;
    public long step;
    public List<Task>[] tasks;

    @SuppressWarnings("unchecked")
    public TimeWheel(int level, long[] dial, long step) {
        this.level = level;
        this.p = new int[level];
        this.dial = dial;
        this.step = step;
        this.tasks = new LinkedList[(int) Arrays.stream(this.dial).asDoubleStream().reduce(1, Double::sum)];
        for (int i = 0; i < this.tasks.length; i++) {
            this.tasks[i] = new LinkedList<>();
        }
        this.init();
    }

    private void init() {
        Runnable r =
                () -> {
                    this.p[0] += this.step;
                    // get effect level
                    //1, just imagine the progress of clock: 23:59:59 --> 00:00:00
                    int e = 0;
                    for (int i = 0; i < this.level; i++) {
                        long quotient = this.p[i] / this.dial[i];
                        long remainder = this.p[i] % this.dial[i];
                        this.p[i] = (int) remainder;
                        if (quotient == 0) {
                            break;
                        } else {
                            this.p[i + 1] += quotient;
                            e = i + 1;
                            if (this.p[this.level - 1] == this.dial[this.level - 1]) {
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
                        long l = this.p[i] + this.dial[i] * i;
                        List<Task> taskList = this.tasks[(int) l];
                        for (Task task : taskList) {
                            int finalI = i;
                            Runnable runnable = () -> {
                                int t = (int) task.unit.toMillis(task.period);
                                double remind = t % (Math.pow(this.dial[finalI], finalI));
                                long position = (long) ((remind + this.p[finalI - 1]) % this.dial[finalI]);
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
            long sleep = TimeUnit.MILLISECONDS.toNanos(this.step);
            //noinspection InfiniteLoopStatement
            while (true) {
                // maybe using parkUntil ?? a abstract time
                LockSupport.parkNanos(sleep);
                ThreadUtil.getThreadPool().submit(r);
            }
        });
    }

    public Task scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
        Consumer<FakeDelayQueue.DelayTask> consumeThisTask = delayTask -> {
            this.scheduleAtFixedRateInner(delayTask.task);
            delayTask.task.runnable.run();
        };
        Task task = Task.of(command, initialDelay, period, unit);
        FakeDelayQueue.DelayTask delayTask = FakeDelayQueue.DelayTask
                .of(task, t -> ThreadUtil.getThreadPool().submit(() -> consumeThisTask.accept(t)));
        if (initialDelay <= 0) {
            consumeThisTask.accept(delayTask);
        } else {
            FakeDelayQueue.delay(delayTask);
        }
        return task;
    }

    private Task scheduleAtFixedRateInner(Task task) {
        int l = (int) task.unit.toMillis(task.period);
        int level = 0;
        int s = 1;
        for (int i = 0; i < this.level; i++) {
            if (l <= this.dial[i] * s) {
                level = i;
                break;
            } else {
                s *= this.dial[i];
            }
        }

        int bucket = (int) (l / Math.pow(this.dial[level], level));

        // insert into relative bucket, not abstract bucket
        long rl = level * this.dial[level] + (bucket + this.p[level]) % this.dial[level];
        this.tasks[(int) rl].add(task);
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
        int j = 1000 * 10;
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
        TimeWheel timeWheel = new TimeWheel(4, new long[]{1000, 60, 60, 24}, 1);
//        IntStream.range(0, i)
//                .forEach(e -> timeWheel.scheduleAtFixedRate(empty, 0, 123, TimeUnit.SECONDS));
//        IntStream.range(0, i)
//                .forEach(e -> timeWheel.scheduleAtFixedRate(empty, 0, 39, TimeUnit.SECONDS));
        IntStream.range(0, j)
                .forEach(e -> timeWheel.scheduleAtFixedRate(r, 5, 3, TimeUnit.SECONDS));

        Thread.sleep(Long.MAX_VALUE);
    }
}
