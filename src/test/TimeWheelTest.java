import org.junit.jupiter.api.Test;
import sun.misc.Unsafe;
import util.thread.FakeDelayQueue;

import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public class TimeWheelTest {
    @Test
    public void testFake() throws InterruptedException {
        Runnable runnable = () -> {
        };

        int i = 1000 * 1000 * 1;
        long l = System.currentTimeMillis();
        IntStream.range(0, i)
                .forEach(e -> {
                    FakeDelayQueue.DelayTask task = FakeDelayQueue.DelayTask.of(runnable, e % 2, 2, TimeUnit.SECONDS, (Void) -> {
                    });
                    FakeDelayQueue.delay(task);
                });
        for (int i1 = 0; i1 < i; i1++) {
            if (!FakeDelayQueue.delayTasks.isEmpty()) {
                FakeDelayQueue.DelayTask poll = FakeDelayQueue.delayTasks.take();
            }
        }

        long time = System.currentTimeMillis();
        System.out.println(TimeUnit.MILLISECONDS.toMillis(time - l));
    }

    @Test
    public void testPark() {
        long s = System.nanoTime();
        while (true) {
            Unsafe.getUnsafe().park(false, 10000);
            long e = System.nanoTime();
            System.out.println(TimeUnit.NANOSECONDS.toMillis(e - s));
            s = e;
        }
    }

}
