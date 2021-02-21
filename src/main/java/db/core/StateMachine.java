package db.core;

import db.operationservice.*;
import db.operationservice.impl.ExpireOperationService;
import db.operationservice.impl.GetOperationService;
import db.operationservice.impl.RemoveOperationService;
import db.operationservice.impl.SetOperationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.LogEntry;
import raft.Node;
import raft.NodeAddress;
import rpc.model.requestresponse.AppendEntriesRequest;
import rpc.model.requestresponse.AppendEntriesResponse;
import rpc.netty.RpcClient;

import java.util.Arrays;
import java.util.List;

/**
 * @author naison
 * @since 4/15/2020 15:18
 */
public class StateMachine {
    private static final Logger LOGGER = LoggerFactory.getLogger(StateMachine.class);

    public static List<Service> services = Arrays.asList(new ExpireOperationService(), new GetOperationService(), new RemoveOperationService(), new SetOperationService());

    /*
     * 发送心跳包，告诉 follower apply log
     * issue: if network split occurs between after leader apply log to db and notify peer to apply log to statemachine
     * how to avoid this issue ?
     * */
    public static void apply(List<LogEntry> entries, Node node) {
        // apply log to db
        for (LogEntry entry : entries) {
            writeLogToStatemachine(node, entry);
        }
        // push commitIndex
        long lastLogIndex = node.getLogEntries().getLastLogIndex();
        node.setCommittedIndex(lastLogIndex);
        node.setLastAppliedIndex(lastLogIndex);

        if (node.isLeader()) {
            // Second round, notify peer to apply log to statemachine
            AppendEntriesRequest request = AppendEntriesRequest.builder()
                    .term(node.getCurrentTerm())
                    .leaderId(node.getLeaderAddress())
                    .prevLogIndex(lastLogIndex)
                    .prevLogTerm(node.getLogEntries().getLastLogTerm())
                    .entries(entries)
                    .leaderCommit(node.getCommittedIndex())
                    .build();
            for (NodeAddress remote : node.allNodeAddressExcludeMe()) {
                node.getMatchIndex().put(remote, lastLogIndex);
                node.getNextIndex().put(remote, lastLogIndex + 1);
                RpcClient.doRequestAsync(remote, request, response -> {
                    if (response != null && ((AppendEntriesResponse) response).isSuccess()) {
                        node.getNextIndex().put(remote, lastLogIndex + 1);
                        node.getMatchIndex().put(remote, lastLogIndex);
                    } else {
                        LOGGER.error("Notify follower to apply log to db occurs error, so try to replicate log");
                        node.leaderReplicateLog();
                    }
                });
            }
        }
    }

    // use strategy mode
    public static void writeLogToStatemachine(Node node, LogEntry entry) {
        for (Service service : services) {
            if (service.supports(entry.getOperation())) {
                service.service(node, entry);
                return;
            }
        }
        LOGGER.error("Operation:" + entry.getOperation() + " is not support");
        throw new UnsupportedOperationException("Operation:" + entry.getOperation() + " is not support");
    }
}
