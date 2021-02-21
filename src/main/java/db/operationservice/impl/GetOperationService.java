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
public class GetOperationService implements Service {
    @Override
    public boolean supports(CURDOperation operation) {
        return CURDOperation.get.equals(operation);
    }

    @Override
    public boolean service(StateMachine stateMachine, LogEntry logEntry) {
        return true;
    }
}
