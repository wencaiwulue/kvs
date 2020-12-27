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
    this.tasks = new LinkedList[(int) (this.dial * this.level)];
    for (int i = 0; i < this.tasks.length; i++) {
      this.tasks[i] = new LinkedList<>();
    }
    init();
  }

  void init() {
    Runnable r =
        () -> {
          //noinspection InfiniteLoopStatement
          while (true) {
            sleepSilently();
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
              // drop task to lower level
              if (quotient == 0) {
                if (i > 0 && offset() > 0) {
                  long l = this.p[i] + this.dial * i;
                  List<Task> taskList = this.tasks[(int) l];
                  for (Task task : taskList) {
                    int t = (int) task.unit.toMillis(task.period);

                    double remind = t % (Math.pow(this.dial, i));
                    // the level which this task should to be put
                    int level = Math.max(0, (int) (Math.log(remind) / Math.log(this.dial)));
                    int bucket = (int) (remind / Math.pow(this.dial, level));

                    long position = level * this.dial + (bucket + this.p[level]) % this.dial;
                    this.tasks[(int) position].add(task);
                  }
                  taskList.clear();
                }
                break;
              } else {
                this.p[i + 1] += quotient;
              }
            }

            List<Task> taskList = this.tasks[this.p[0]];
            for (Task task : taskList) {
              ThreadUtil.getThreadPool().execute(task.runnable);
              this.scheduleAtFixedRate(task.runnable, 0, task.period, task.unit);
            }
            taskList.clear();
          }
        };
    ThreadUtil.getThreadPool().execute(r);
    //    new Thread(r).start();
  }

  private void sleepSilently() {
    try {
      Thread.sleep(1);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
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

  public long offset() {
    long a = 0;
    for (int i = this.level - 1; i > 0; i--) {
      if (this.p[i] > 0) {
        return i * this.dial + this.p[i];
      }
    }
    return a;
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
    TimeWheel timeWheel = new TimeWheel(3, 60, 1);
    timeWheel.scheduleAtFixedRate(runnable, 0, 5, TimeUnit.SECONDS);
    Thread.sleep(1000 * 1000);
  }
}
