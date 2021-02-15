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
import rpc.netty.RpcClient;

import java.util.Arrays;
import java.util.Collections;
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
     * issue: if network split occurs between after leader apply log to db and notify peer to apply log to db
     * how to avoid this issue ?
     * */
    public static void apply(List<LogEntry> entries, Node node) {
        // apply log to db
        for (LogEntry entry : entries) {
            writeLogToDB(node, entry);
        }
        // push commitIndex
        node.setCommittedIndex(entries.get(entries.size() - 1).getIndex());
        if (node.isLeader()) {
            // notify peer to apply log to db
            AppendEntriesRequest request = new AppendEntriesRequest(Collections.emptyList(), node.getLocalAddress(), node.getCurrentTerm(), node.getLastAppliedTerm(), node.getLastAppliedTerm(), node.getCommittedIndex());
            for (NodeAddress remote : node.allNodeAddressExcludeMe()) {
                RpcClient.doRequestAsync(remote, request, e -> {
                });
            }
        }
    }

    // use strategy mode
    public static void writeLogToDB(Node node, LogEntry entry) {
        for (Service service : services) {
            if (service.supports(entry.getOperation())) {
                service.service(node, entry);
                node.getLogdb().remove(entry.getIndex());
                return;
            }
        }
        LOGGER.error("Operation:" + entry.getOperation() + " is not support");
        throw new UnsupportedOperationException("Operation:" + entry.getOperation() + " is not support");
    }
}
