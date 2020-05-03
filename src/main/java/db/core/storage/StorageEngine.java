package db.core.storage;

import java.util.Iterator;

/**
 * storage engine, can use Redis、HashMap、LevelDB or RocksDB etc
 *
 * @author naison
 * @since 5/3/2020 17:39
 */
public interface StorageEngine {

    <T> T get(String key);

    <T> boolean set(String key, T t);

    <T> boolean remove(String key);

    <T> Iterator<T> iterator();

}
