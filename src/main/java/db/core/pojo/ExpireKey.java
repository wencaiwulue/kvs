package db.core.pojo;

import org.jetbrains.annotations.NotNull;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 * @author naison
 * @since 4/1/2020 11:07
 */
public class ExpireKey implements Delayed {
    private final long future;
    private final TimeUnit unit;
    private final String key;

    public ExpireKey(long time, TimeUnit unit, String key) {
        this.future = unit.toMillis(time) + System.currentTimeMillis();
        this.unit = TimeUnit.MILLISECONDS;
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    @Override
    public long getDelay(@NotNull TimeUnit unit) {
        return unit.convert(this.future - System.currentTimeMillis(), this.unit);
    }

    @Override
    public int compareTo(@NotNull Delayed o) {
        return Long.compare(this.getDelay(TimeUnit.MILLISECONDS), o.getDelay(TimeUnit.MILLISECONDS));
    }

}
