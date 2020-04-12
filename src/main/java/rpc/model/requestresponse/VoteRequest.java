package rpc.model.requestresponse;

import lombok.Data;
import rpc.model.Request;

/**
 * @author naison
 * @since 4/12/2020 14:54
 */
@Data
public class VoteRequest extends Request {
    private int term;
    private int lastLogIndex;
    private int lastLogTerm;

    public VoteRequest(int term, int lastLogIndex, int lastLogTerm) {
        this.term = term;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
    }

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
