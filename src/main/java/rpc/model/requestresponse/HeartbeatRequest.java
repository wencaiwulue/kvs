package rpc.model.requestresponse;

import lombok.Data;
import lombok.ToString;
import raft.NodeAddress;

/**
 * @author naison
 * @since 4/12/2020 16:53
 */
@Data
@ToString
public class HeartbeatRequest extends Request {
    private static final long serialVersionUID = 6824938127696128332L;
    int term;
    NodeAddress leaderAddr;

    public HeartbeatRequest(int term, NodeAddress leaderAddr) {
        this.term = term;
        this.leaderAddr = leaderAddr;
    }

    public int getTerm() {
        return term;
    }

    public NodeAddress getLeaderAddr() {
        return leaderAddr;
    }
}
