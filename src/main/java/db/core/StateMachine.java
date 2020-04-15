package db.core;

import raft.LogEntry;
import raft.Node;

import java.util.List;

/**
 * @author naison
 * @since 4/15/2020 15:18
 */
public class StateMachine {
    public static void apply(List<LogEntry> entries, Node leader) {
        // todo 发送一个心跳包，告诉follower提交
    }
}
