package util.thread;

import java.util.concurrent.TimeUnit;

public interface ScheduleService {
    void cancel();

    TimeWheel.Task scheduleAtFixedRate(
            Runnable command, long initialDelay, long period, TimeUnit unit);
}
