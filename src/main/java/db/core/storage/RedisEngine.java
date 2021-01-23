package db.core.storage;

import redis.clients.jedis.Jedis;
import util.FSTUtil;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * @author naison
 * @since 5/3/2020 18:32
 */

public class RedisEngine implements StorageEngine {
    private static final Jedis JEDIS;

    static {
        JEDIS = new Jedis("localhost", 6379);
    }

    public RedisEngine() {
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(String key) {
        String s = JEDIS.get(key);
        if (s != null) {
            return (T) FSTUtil.getBinaryConf().asObject(s.getBytes());
        } else {
            return null;
        }
    }

    @Override
    public <T> boolean set(String key, T t) {
        JEDIS.set(key, FSTUtil.getBinaryConf().asJsonString(t));
        return true;
    }

    @Override
    public <T> boolean remove(String key) {
        JEDIS.del(key);
        return true;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Iterator<T> iterator() {
        Set<String> keys = JEDIS.keys("*");
        Map<String, Object> map = new HashMap<>();
        keys.parallelStream().forEach(e -> {
            String s = JEDIS.get(e);
            Object o = FSTUtil.getBinaryConf().asObject(s.getBytes());
            map.put(e, o);
        });
        return (Iterator<T>) map.entrySet().iterator();
    }
}
