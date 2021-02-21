package db.operationservice.impl;

import db.core.StateMachine;
import db.operationservice.Service;
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
    public boolean service(StateMachine stateMachine, LogEntry logEntry) {
        stateMachine.remove(logEntry.getKey());
        return true;
    }
}
