package db.operationservice;

import raft.LogEntry;
import raft.Node;
import raft.enums.CURDOperation;

/**
 * @author naison
 * @since 4/27/2020 14:06
 */
public class RemoveOperationService implements Service {
    @Override
    public boolean supports(CURDOperation operation) {
        return CURDOperation.remove.equals(operation);
    }

    @Override
    public boolean service(Node node, LogEntry logEntry) {
        node.db.remove(logEntry.getKey());
        return true;
    }
}
