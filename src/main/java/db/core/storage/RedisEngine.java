package db.core.storage;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import util.FSTUtil;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * @author naison
 * @since 5/3/2020 18:32
 */

public class RedisEngine<K, V> implements StorageEngine<K, V> {
    private static final Jedis JEDIS;

    static {
        JEDIS = new Jedis("localhost", 6379);
    }

    public RedisEngine() {
    }

    @Override
    public V get(K key) {
        String s = JEDIS.get(String.valueOf(key));
        if (s != null) {
            @SuppressWarnings("unchecked")
            V o = (V) FSTUtil.getBinaryConf().asObject(s.getBytes());
            return o;
        } else {
            return null;
        }
    }

    @Override
    public boolean set(K key, V v) {
        JEDIS.set(String.valueOf(key), FSTUtil.getBinaryConf().asJsonString(v));
        return true;
    }

    @Override
    public boolean remove(K key) {
        JEDIS.del(String.valueOf(key));
        return true;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Iterator<Map.Entry<K, V>> iterator() {
        Set<String> keys = JEDIS.keys("*");
        Map<K, V> map = new HashMap<>(keys.size());
        Pipeline pipeline = JEDIS.pipelined();
        keys.parallelStream().forEach(e -> {
            String s = pipeline.get(e).get();
            V o = (V) FSTUtil.getBinaryConf().asObject(s.getBytes());
            map.put((K) e, o);
        });
        return map.entrySet().iterator();
    }
}
