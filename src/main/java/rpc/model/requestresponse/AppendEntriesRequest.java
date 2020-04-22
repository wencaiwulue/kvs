package rpc.model.requestresponse;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import raft.LogEntry;
import raft.NodeAddress;

import java.util.List;

/**
 * @author naison
 * @since 4/13/2020 14:05
 */
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class AppendEntriesRequest extends Request {
    private static final long serialVersionUID = -2322012843577274410L;

    private List<LogEntry> entries;// if it is empty, means it's a heartbeat
    private NodeAddress leaderId;
    private int term;
    private int prevLogTerm;
    private long prevLogIndex;
    private long committedIndex;
}
