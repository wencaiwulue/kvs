package rpc.model.requestresponse;

import lombok.AllArgsConstructor;
import lombok.Data;
import raft.NodeAddress;

import java.net.InetSocketAddress;

/**
 * @author naison
 * @since 4/12/2020 14:54
 */
@Data
@AllArgsConstructor
public class VoteRequest extends Request {
    private static final long serialVersionUID = -6056301287980072876L;
    private NodeAddress candidateId;
    private int term;
    private int lastLogIndex;
    private int lastLogTerm;

    public int getTerm() {
        return term;
    }

    public int getLastLogIndex() {
        return lastLogIndex;
    }

    public int getLastLogTerm() {
        return lastLogTerm;
    }
}
