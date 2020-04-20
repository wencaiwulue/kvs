package rpc.model.requestresponse;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import raft.NodeAddress;

import java.net.InetSocketAddress;

/**
 * @author naison
 * @since 4/12/2020 15:12
 */
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class AddPeerRequest extends Request {
    private static final long serialVersionUID = -4132647511647067775L;

    private NodeAddress peer;
    public NodeAddress sender;

    public AddPeerRequest(NodeAddress peer) {
        this.peer = peer;
    }

    public NodeAddress getPeer() {
        return peer;
    }
}
