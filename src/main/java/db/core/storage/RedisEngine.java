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
    private static final Jedis jedis;

    static {
        jedis = new Jedis("localhost", 6379);
    }

    @Override
    public <T> T get(String key) {
        String s = jedis.get(key);
        if (s != null) {
            return (T) FSTUtil.getConf().asObject(s.getBytes());
        } else {
            return null;
        }
    }

    @Override
    public <T> boolean set(String key, T t) {
        jedis.set(key, FSTUtil.getConf().asJsonString(t));
        return true;
    }

    @Override
    public <T> boolean remove(String key) {
        jedis.del(key);
        return true;
    }

    @Override
    public <T> Iterator<T> iterator() {
        Set<String> keys = jedis.keys("*");
        Map<String, Object> map = new HashMap<>();
        keys.parallelStream().forEach(e -> {
            String s = jedis.get(e);
            Object o = FSTUtil.getConf().asObject(s.getBytes());
            map.put(e, o);
        });
        return (Iterator<T>) map.entrySet().iterator();
    }
}
