package rpc.model.requestresponse;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import raft.LogEntry;
import raft.NodeAddress;

import java.util.Collections;
import java.util.List;

/**
 * @author naison
 * @since 4/13/2020 14:05
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class AppendEntriesRequest extends Request {
    private static final long serialVersionUID = -2322012843577274410L;

    private int term;
    private NodeAddress leaderId;
    private long prevLogIndex;
    private int prevLogTerm;
    private List<LogEntry> entries = Collections.emptyList();// if it is empty, means it's a heartbeat
    private long leaderCommit;
}
