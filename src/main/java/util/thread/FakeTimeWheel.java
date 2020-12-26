package util.thread;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import util.ThreadUtil;

import java.util.Comparator;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;

public class FakeTimeWheel {
  PriorityBlockingQueue<Task> tasks =
      new PriorityBlockingQueue<>(1000 * 1000, Comparator.comparingLong(Task::nextRunTime));

  {
    Runnable r =
        () -> {
          //noinspection InfiniteLoopStatement
          while (true) {
            Task poll = tasks.poll();
            if (poll != null) {
              if (poll.nextRunTime <= System.currentTimeMillis()) {
                ThreadUtil.getThreadPool().submit(poll.runnable);
                poll.nextRunTime = poll.nextRunTime();
                tasks.offer(poll);
              } else {
                tasks.offer(poll);
              }
            }
          }
        };
    ThreadUtil.getThreadPool().submit(r);
  }

  public Task scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
    Task task = Task.of(command, initialDelay, period, unit);
    tasks.offer(task);
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
    Runnable runnable =
        () -> System.out.println("current timestamp: " + System.currentTimeMillis() / 1000);
    FakeTimeWheel timeWheel = new FakeTimeWheel();
    timeWheel.scheduleAtFixedRate(runnable, 1, 2, TimeUnit.SECONDS);
    Thread.sleep(1000 * 1000);
  }
}
