package db.core.storage;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author naison
 * @since 4/28/2020 18:03
 */

public class MapStorage<K, V> implements StorageEngine<K, V> {
    // this can be replaced to RocksDB、LevelDB or B-tree
    public final ConcurrentHashMap<K, V> map;

    public MapStorage() {
        // 1 << 30 is hashmap max capacity
        this.map = new ConcurrentHashMap<>(1 << 30);
    }

    @Override
    public V get(K key) {
        return this.map.get(key);
    }

    @Override
    public boolean set(K key, V v) {
        this.map.put(key, v);
        return true;
    }

    @Override
    public boolean remove(K key) {
        this.map.remove(key);
        return true;
    }

    public Iterator<Map.Entry<K, V>> iterator() {
        return map.entrySet().iterator();
    }

}
