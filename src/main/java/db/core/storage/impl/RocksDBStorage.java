package db.core.storage.impl;

import db.core.storage.StorageEngine;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map;

public class RocksDBStorage implements StorageEngine<byte[], byte[]> {
    private static final Logger LOG = LoggerFactory.getLogger(RocksDBStorage.class);

    private RocksDB rocksDB;

    public RocksDBStorage(Path dbFolder) {
        try {
            if (!dbFolder.toFile().exists()) {
                if (!dbFolder.toFile().mkdirs()) {
                    LOG.error("Create dir error, dir: {}", dbFolder);
                }
            }
            rocksDB = RocksDB.open(dbFolder.toAbsolutePath().toString());
        } catch (RocksDBException ex) {
            LOG.error(ex.getMessage());
        }
    }


    public boolean setBatch(byte[][] keys, byte[][] values) {
        try (final WriteBatch batch = new WriteBatch();
             final WriteOptions wOpt = new WriteOptions()) {
            for (int i = 0; i < keys.length; i++) {
                batch.put(keys[i], values[i]);
            }
            rocksDB.write(wOpt, batch);
            return true;
        } catch (RocksDBException exception) {
            return false;
        }
    }

    @Override
    public byte[] get(byte[] key) {
        try {
            return rocksDB.get(key);
        } catch (RocksDBException e) {
            return new byte[0];
        }
    }

    @Override
    public boolean set(byte[] key, byte[] t) {
        try {
            rocksDB.put(key, t);
            return true;
        } catch (RocksDBException e) {
            return false;
        }
    }

    @Override
    public boolean remove(byte[] key) {
        try {
            rocksDB.delete(key);
            return true;
        } catch (RocksDBException e) {
            return false;
        }
    }

    @Override
    public Iterator<Map.Entry<byte[], byte[]>> iterator() {
        RocksIterator iterator = rocksDB.newIterator();
        return null;
    }
}
