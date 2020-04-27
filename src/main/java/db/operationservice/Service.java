package db.operationservice;

import raft.LogEntry;
import raft.Node;
import raft.enums.CURDOperation;

/**
 * @author naison
 * @since 4/27/2020 14:08
 */
public interface Service {

    boolean support(CURDOperation operation);

    boolean service(Node node, LogEntry logEntry);

}
