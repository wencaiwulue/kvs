package rpc.model.requestresponse;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import raft.LogEntry;
import raft.NodeAddress;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * @author naison
 * @since 4/13/2020 14:05
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AppendEntriesRequest extends Request {
    private static final long serialVersionUID = -2322012843577274410L;
    public List<LogEntry> entries;
    public NodeAddress leaderAddress;
    public int term;
}
