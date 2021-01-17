import org.junit.jupiter.api.Test;
import sun.misc.Unsafe;
import util.thread.FakeDelayQueue;
import util.thread.TimeWheel;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

public class TimeWheelTest {
    @Test
    public void testFake() throws InterruptedException {
        Runnable runnable = () -> {
        };

        int i = 1000 * 1000 * 1;
        long l = System.currentTimeMillis();
        IntStream.range(0, i)
                .forEach(e -> FakeDelayQueue.delay(FakeDelayQueue.DelayTask.of(TimeWheel.Task.of(runnable, e % 2, 2, TimeUnit.SECONDS), (Void) -> {
                })));
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

    @Test
    public void testPerformance() throws InterruptedException {
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
