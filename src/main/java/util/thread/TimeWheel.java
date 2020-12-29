package util.thread;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import util.ThreadUtil;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
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
    }

    {
        Runnable r =
                () -> {
                    this.p[0] += 1;

                    // get effect level
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
                                // clear all
                                for (int j = 0; j < this.level; j++) {
                                    this.p[j] = 0;
                                }
                                break;
                            }
                        }
                    }

                    for (int i = e; i > 0; i--) {
                        long l = this.p[i] + this.dial * i;
                        List<Task> taskList = this.tasks[(int) l];
                        for (Task task : taskList) {
                            int t = (int) task.unit.toMillis(task.period);
                            double remind = t % (Math.pow(this.dial, i));
                            long position = (long) ((remind + this.p[i - 1]) % this.dial);
                            this.tasks[(int) position].add(task);
                        }
                        taskList.clear();
                    }

                    List<Task> taskList = this.tasks[this.p[0]];
                    for (Task task : taskList) {
                        ThreadUtil.getThreadPool().execute(task.runnable);
                        this.scheduleAtFixedRate(task.runnable, 0, task.period, task.unit);
                    }
                    taskList.clear();
                };

        FakeTimeWheel fakeTimeWheel = new FakeTimeWheel();
        fakeTimeWheel.scheduleAtFixedRate(r, 0, 1, TimeUnit.MILLISECONDS);
    }

    public Task scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
        Task task = Task.of(command, initialDelay, period, unit);
        int l = (int) unit.toMillis(initialDelay + period);

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
        Runnable runnable =
                () -> System.out.println("current timestamp: " + System.currentTimeMillis() / 1000);
        TimeWheel timeWheel = new TimeWheel(2, 60, 1);
        timeWheel.scheduleAtFixedRate(runnable, 0, 2, TimeUnit.SECONDS);
//        Thread.sleep(5);
        timeWheel.scheduleAtFixedRate(() -> System.out.println(System.currentTimeMillis() / 1000), 0, 1, TimeUnit.SECONDS);
        IntStream.range(0, 1000 * 100)
                .forEach(e -> timeWheel.scheduleAtFixedRate(() -> {
                }, 0, 1, TimeUnit.SECONDS));

        IntStream.range(0, 1000 * 100)
                .forEach(e -> timeWheel.scheduleAtFixedRate(() -> {
                }, 0, 2, TimeUnit.SECONDS));

        IntStream.range(0, 1000 * 100)
                .forEach(e -> timeWheel.scheduleAtFixedRate(() -> {
                }, 0, 3, TimeUnit.SECONDS));
        Random random = new Random();
        for (int i = 10; i > 0; i--) {
            System.out.println(random.doubles());
        }
        Thread.sleep(1000 * 1000000);
    }
}
