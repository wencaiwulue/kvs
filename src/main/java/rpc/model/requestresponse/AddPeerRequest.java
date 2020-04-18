package rpc.model.requestresponse;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import raft.NodeAddress;

import java.net.InetSocketAddress;

/**
 * @author naison
 * @since 4/12/2020 15:12
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AddPeerRequest extends Request {
    private static final long serialVersionUID = -4132647511647067775L;
    public NodeAddress peer;
    public NodeAddress sender;

    public AddPeerRequest(NodeAddress peer) {
        this.peer = peer;
    }

    public NodeAddress getPeer() {
        return peer;
    }
}
