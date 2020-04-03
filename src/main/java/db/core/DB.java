package db.core;

import thread.ThreadUtil;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * @author naison
 * @since 4/1/2020 10:53
 */
public class DB {
    private ConcurrentHashMap<String, Object> map;
    private MinPQ<ExpireKey> expireKeys;

    public DB() {
        this.map = new ConcurrentHashMap<>();
        this.expireKeys = new MinPQ<>();
        checkExpireKey();
    }

    private void checkExpireKey() {
        // check key expire every seconds
        Runnable runnable = () -> {
            if (!expireKeys.isEmpty()) {
                ExpireKey expireKey = expireKeys.peekMin();
                if (System.nanoTime() >= expireKey.getExpire()) {
                    map.remove(expireKey.getKey());
                    expireKeys.delMin();
                }
            }
        };

        ThreadUtil.getSchedulePool().scheduleAtFixedRate(runnable, 0, 1, TimeUnit.SECONDS);
    }

    public Object get(String key) {
        return map.get(key);
    }

    public void set(String key, Object value) {
        set(key, value, 0, TimeUnit.MILLISECONDS);
    }

    public void set(String key, Object value, int timeout, TimeUnit unit) {
        if (key == null) return;
        map.put(key, value);
        if (timeout > 0) {
            expireKeys.insert(new ExpireKey(key, System.nanoTime() + unit.toNanos(timeout)));
        }
    }

    public void remove(String key) {
        map.remove(key);// expire key 可以不用删除
    }

    public void expireKey(String key, int expire, TimeUnit unit) {
        // 如果已经在expireKey中，就会出问题。处理方法是加反向索引，但是这样值得吗
    }

    public static void main(String[] args) {
        DB db = new DB();
        for (int i = 4; i > 0; i--) {
            db.set(String.valueOf(i), i, i, TimeUnit.NANOSECONDS);
        }
        System.out.println(db);
    }

}
