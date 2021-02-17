package db.core.storage;

import java.util.Iterator;
import java.util.Map;

/**
 * storage engine, can use Redis、HashMap、LevelDB or RocksDB etc
 *
 * @author naison
 * @since 5/3/2020 17:39
 */
public interface StorageEngine<K, V> {

    V get(K key);

    boolean set(K key, V t);

    boolean remove(K key);

    default void removeRange(K keyInclusive, K keyExclusive) {}

    Iterator<Map.Entry<K, V>> iterator();

}
