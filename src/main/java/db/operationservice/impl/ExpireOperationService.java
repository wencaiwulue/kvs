package db.operationservice.impl;

import db.core.StateMachine;
import db.operationservice.Service;
import raft.LogEntry;
import raft.Node;
import raft.enums.CURDOperation;

import java.util.concurrent.TimeUnit;

/**
 * @author naison
 * @since 4/27/2020 14:06
 */
public class ExpireOperationService implements Service {
    @Override
    public boolean supports(CURDOperation operation) {
        return CURDOperation.expire.equals(operation);
    }

    @Override
    public boolean service(StateMachine stateMachine, LogEntry logEntry) {
        stateMachine.expireKey(logEntry.getKey(), (int) logEntry.getValue(), TimeUnit.MILLISECONDS);
        return false;
    }
}
