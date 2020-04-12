package rpc.model.requestresponse;

import lombok.Data;
import rpc.model.Request;

import java.net.InetSocketAddress;

/**
 * @author naison
 * @since 4/12/2020 16:53
 */
@Data
public class HeartbeatRequest extends Request {
    int term;
    InetSocketAddress leaderAddr;

    public HeartbeatRequest(int term, InetSocketAddress leaderAddr) {
        this.term = term;
        this.leaderAddr = leaderAddr;
    }

    public int getTerm() {
        return term;
    }

    public InetSocketAddress getLeaderAddr() {
        return leaderAddr;
    }
}
