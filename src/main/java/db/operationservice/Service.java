package db.operationservice;

import db.core.StateMachine;
import raft.LogEntry;
import raft.Node;
import raft.enums.CURDOperation;

/**
 * @author naison
 * @since 4/27/2020 14:08
 */
public interface Service {

    boolean supports(CURDOperation operation);

    boolean service(StateMachine stateMachine, LogEntry logEntry);

}
