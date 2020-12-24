package util;

import lombok.*;

import java.util.Comparator;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class FakeTimeWheel {
  PriorityBlockingQueue<Task> tasks =
      new PriorityBlockingQueue<Task>(1000 * 1000, Comparator.comparingLong(Task::nextRunTime));

  public ScheduledFuture<?> scheduleAtFixedRate(
      Runnable command, long initialDelay, long period, TimeUnit unit) {
    Task task = Task.of(command, period, unit);
    tasks.offer(task);
    return task;
  }

  @AllArgsConstructor
  @NoArgsConstructor
  @Getter
  @Setter
  public static class Task {
    public Runnable runnable;
    public long period;
    public TimeUnit unit;

    public Long nextRunTime() {
      return System.currentTimeMillis() + unit.toMillis(period);
    }

    public static Task of(Runnable runnable, long period, TimeUnit unit) {
      return new Task(runnable, period, unit);
    }
  }
}
