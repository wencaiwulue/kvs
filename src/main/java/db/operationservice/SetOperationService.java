package db.operationservice;

import raft.LogEntry;
import raft.Node;
import raft.enums.CURDOperation;

/**
 * @author naison
 * @since 4/27/2020 14:06
 */
public class SetOperationService implements Service {
    @Override
    public boolean supports(CURDOperation operation) {
        return CURDOperation.set.equals(operation);
    }

    @Override
    public boolean service(Node node, LogEntry logEntry) {
        node.db.set(logEntry.getKey(), logEntry.getValue());
        return true;
    }
}
