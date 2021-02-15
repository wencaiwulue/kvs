package rpc.model.requestresponse;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import raft.NodeAddress;

/**
 * @author naison
 * @since 4/12/2020 14:54
 */
@Getter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class VoteRequest extends Request {
    private static final long serialVersionUID = -6056301287980072876L;
    private int term;
    private NodeAddress candidateId;
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
