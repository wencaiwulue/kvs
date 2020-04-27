package db.operationservice;

import raft.LogEntry;
import raft.Node;
import raft.enums.CURDOperation;

/**
 * @author naison
 * @since 4/27/2020 14:06
 */
public class GetOperationService implements Service {
    @Override
    public boolean support(CURDOperation operation) {
        return CURDOperation.get.equals(operation);
    }

    @Override
    public boolean service(Node node, LogEntry logEntry) {
        return true;
    }
}
