package db.core;

import raft.LogEntry;
import raft.Node;
import raft.NodeAddress;
import rpc.Client;
import rpc.model.requestresponse.AppendEntriesRequest;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author naison
 * @since 4/15/2020 15:18
 */
public class StateMachine {

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
            Client.doRequest(remote, request);
        }
    }

    public static void writeLogToDB(Node leader, LogEntry entry) {
        switch (entry.getOperation()) {
            case set: {
                leader.db.set(entry.getKey(), entry.getValue());
                break;
            }
            case remove: {
                leader.db.remove(entry.getKey());
                break;
            }
            case expire: {
                leader.db.expireKey(entry.getKey(), (int) entry.getValue(), TimeUnit.MILLISECONDS);
                break;
            }
            default:
                Node.log.error("Operation:" + entry.getOperation() + " is not support.");
                throw new UnsupportedOperationException("Operation:" + entry.getOperation() + " is not support.");
        }
    }
}
