package db.core.storage;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;

/**
 * @author naison
 * @since 4/28/2020 18:03
 */
public class MapStorage implements StorageEngine {
    public final ConcurrentHashMap<String, Object> map;// this can be replaced to RockDB、LevelDB or B-tree

    private final Lock writeLock;

    public MapStorage(Lock lock) {
        this.writeLock = lock;
        this.map = new ConcurrentHashMap<>(/*1 << 30*/); // 这是hashMap的容量
    }

    @Override
    public <T> T get(String key) {
        return (T) this.map.get(key);
    }

    @Override
    public <T> boolean set(String key, T t) {
        this.writeLock.lock();
        try {
            this.map.put(key, t);
            return true;
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public <T> boolean remove(String key) {
        this.writeLock.lock();
        try {
            this.map.remove(key);
            return true;
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public <T> Iterator<T> iterator() {
        return (Iterator<T>) map.entrySet().iterator();
    }

}
