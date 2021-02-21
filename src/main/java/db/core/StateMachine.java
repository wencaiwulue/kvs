package db.core;

import db.core.storage.StorageEngine;
import db.operationservice.Service;
import db.operationservice.impl.ExpireOperationService;
import db.operationservice.impl.GetOperationService;
import db.operationservice.impl.RemoveOperationService;
import db.operationservice.impl.SetOperationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.LogEntry;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author naison
 * @since 4/15/2020 15:18
 */
public class StateMachine {
    private static final Logger LOGGER = LoggerFactory.getLogger(StateMachine.class);

    public static List<Service> services = Arrays.asList(new ExpireOperationService(), new GetOperationService(), new RemoveOperationService(), new SetOperationService());

    private final DB storage;

    public StateMachine(DB storage) {
        this.storage = storage;
    }

    public void applyLog(List<LogEntry> entry) {
        for (LogEntry logEntry : entry) {
            this.applyLog(logEntry);
        }
    }

    // use strategy mode
    private void applyLog(LogEntry entry) {
        for (Service service : services) {
            if (service.supports(entry.getOperation())) {
                service.service(this, entry);
                return;
            }
        }
        LOGGER.error("Operation:" + entry.getOperation() + " is not support");
        throw new UnsupportedOperationException("Operation:" + entry.getOperation() + " is not support");
    }

    public void expireKey(String key, int expire, TimeUnit unit) {
        this.storage.expireKey(key, expire, unit);
    }

    public Object get(String key) {
        return this.storage.get(key);
    }

    public void remove(String key) {
        this.storage.remove(key);
    }

    public void set(String key, Object value) {
        this.storage.set(key, value);
    }

    public StorageEngine<?, ?> getDb() {
        return this.storage.getStorage();
    }
}
