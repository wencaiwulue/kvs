package db.core;

/**
 * @author naison
 * @since 4/1/2020 11:07
 */
public class ExpireKey implements Comparable {
    private String key;
    private long expire;// System.nanoTime();

    public ExpireKey() {
    }

    public ExpireKey(String key, long expire) {
        this.key = key;
        this.expire = expire;
    }

    public String getKey() {
        return key;
    }

    public long getExpire() {
        return expire;
    }

    @Override
    public int compareTo(Object o) {
        return Long.compare(expire, ((ExpireKey) o).expire);
    }
}
