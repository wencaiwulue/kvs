package rpc.model.requestresponse;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import raft.LogEntry;
import raft.NodeAddress;

/**
 * @author naison
 * @since 4/14/2020 10:52
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class InstallSnapshotRequest extends Request {
    private static final long serialVersionUID = -8137679486480941058L;

    private int term;
    private NodeAddress leaderId;
    private int lastIncludedIndex;
    private int lastIncludedTerm;
    private int offset;
    private LogEntry[] data;
    private boolean done;
}
