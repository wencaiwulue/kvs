package db.core.storage;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author naison
 * @since 4/28/2020 18:03
 */

public class MapStorage implements StorageEngine {
    public final ConcurrentHashMap<String, Object> map;// this can be replaced to RockDB、LevelDB or B-tree

    public MapStorage() {
        this.map = new ConcurrentHashMap<>(/*1 << 30*/); // 这是hashMap的容量
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(String key) {
        return (T) this.map.get(key);
    }

    @Override
    public <T> boolean set(String key, T t) {
        this.map.put(key, t);
        return true;
    }

    @Override
    public <T> boolean remove(String key) {
        this.map.remove(key);
        return true;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Iterator<T> iterator() {
        return (Iterator<T>) map.entrySet().iterator();
    }

}
