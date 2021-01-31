package rpc.model.requestresponse;

import lombok.*;
import raft.NodeAddress;

import java.net.InetSocketAddress;

/**
 * @author naison
 * @since 4/12/2020 15:12
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class AddPeerRequest extends Request {
    private static final long serialVersionUID = -4132647511647067775L;

    private NodeAddress peer;
    public NodeAddress sender;

    public AddPeerRequest(NodeAddress peer) {
        this.peer = peer;
    }

}
