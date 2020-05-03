package db.core;

import java.util.concurrent.TimeUnit;

/**
 * @author naison
 * @since 4/1/2020 11:07
 */
public class ExpireKey implements Comparable<ExpireKey> {
    private String key;
    private long expire;// System.nanoTime();

    public ExpireKey() {
    }

    public ExpireKey(String key, int expire, TimeUnit unit) {
        this.key = key;
        if (expire > 0 && unit != null) {
            this.expire = unit.toNanos(expire) + System.nanoTime();
        } else {
            this.expire = -1;
        }
    }

    public String getKey() {
        return this.key;
    }

    public long getExpire() {
        return this.expire;
    }

    public boolean isExpired() {
        return this.expire < System.nanoTime();
    }

    @Override
    public int compareTo(ExpireKey o) {
        if (o.expire > 0 && this.expire > 0) {
            return Long.compare(this.expire, o.expire);
        } else {
            return key.compareTo(o.key);
        }
    }
}
