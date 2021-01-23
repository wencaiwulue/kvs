package db.core;

import db.operationservice.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.LogEntry;
import raft.Node;
import raft.NodeAddress;
import rpc.model.requestresponse.AppendEntriesRequest;
import rpc.netty.pub.RpcClient;

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
     * */
    public static void apply(List<LogEntry> entries, Node leader) {
        for (LogEntry entry : entries) {
            writeLogToDB(leader, entry);
        }
        leader.committedIndex = entries.get(entries.size() - 1).index;

        AppendEntriesRequest request = new AppendEntriesRequest(Collections.emptyList(), leader.address, leader.currentTerm, leader.getLastAppliedTerm(), leader.getLastAppliedTerm(), leader.committedIndex);
        for (NodeAddress remote : leader.allNodeAddressExcludeMe()) {
            RpcClient.doRequest(remote, request);
        }
    }

    // use strategy mode
    public static void writeLogToDB(Node leader, LogEntry entry) {

        for (Service service : services) {
            if (service.supports(entry.getOperation())) {
                service.service(leader, entry);
                return;
            }
        }

        LOGGER.error("Operation:" + entry.getOperation() + " is not support.");
        throw new UnsupportedOperationException("Operation:" + entry.getOperation() + " is not support.");
    }
}
