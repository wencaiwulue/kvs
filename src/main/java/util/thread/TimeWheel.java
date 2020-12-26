package util.thread;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import util.ThreadUtil;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class TimeWheel {
    public int level;
    public int[] p;
    public long dial;
    public long step;
    public List<Task>[] tasks;

    public TimeWheel(int level, long dial, long step) {
        this.level = level;
        this.p = new int[level];
        this.dial = dial;
        this.step = step;
        this.tasks = new LinkedList[(int) Math.pow(this.dial, this.level)];
        for (int i = 0; i < this.tasks.length; i++) {
            this.tasks[i] = new LinkedList<>();
        }
        init();
    }

    private void init() {
        Runnable r = () -> {
            //noinspection InfiniteLoopStatement
            while (true) {
                sleepSilently(1);
                this.p[0] += 1;

                for (int i = 0; i < this.level; i++) {
                    if (this.p[this.level - 1] == this.dial) {
                        // clear all
                        for (int j = 0; j < this.level; j++) {
                            this.p[j] = 0;
                        }
                        break;
                    }
                    long quotient = this.p[i] / this.dial;
                    long remainder = this.p[i] % this.dial;
                    this.p[i] = (int) remainder;
                    this.p[i + 1] += quotient;
                    if (quotient == 0) {
                        break;
                    }
                }

                int point = 0;
                for (int i = 0; i < this.level; i++) {
                    point += this.p[i] * Math.pow(this.dial, i);
                }

                for (Task task : this.tasks[point]) {
                    ThreadUtil.getThreadPool().submit(task.runnable);
                    int l = (int) task.unit.toSeconds(task.period);
                    this.tasks[l].add(task);
                }
            }
        };
        ThreadUtil.getThreadPool().submit(r);
    }

    private void sleepSilently(int second) {
        try {
            Thread.sleep(second * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public Task scheduleAtFixedRate(
            Runnable command, long initialDelay, long period, TimeUnit unit) {
        Task task = Task.of(command, initialDelay, period, unit);
        int l = (int) unit.toSeconds(initialDelay + period);
        this.tasks[l].add(task);
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

        public long nextRunTime;

        public Long nextRunTime() {
            return System.currentTimeMillis() + unit.toMillis(period);
        }

        public static Task of(Runnable runnable, long initialDelay, long period, TimeUnit unit) {
            long firstTime = System.currentTimeMillis() + unit.toMillis(initialDelay);
            return new Task(runnable, initialDelay, period, unit, firstTime);
        }
    }

    public static void main(String[] args) throws InterruptedException {
        Runnable runnable = () -> System.out.println("current timestamp: " + System.currentTimeMillis() / 1000);
        TimeWheel timeWheel = new TimeWheel(3, 60, 1);
        timeWheel.scheduleAtFixedRate(runnable, 1, 3, TimeUnit.SECONDS);
        Thread.sleep(1000 * 1000);
    }
}
