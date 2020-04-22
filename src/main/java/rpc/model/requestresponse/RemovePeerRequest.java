package rpc.model.requestresponse;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import raft.NodeAddress;

/**
 * @author naison
 * @since 4/12/2020 15:13
 */
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class RemovePeerRequest extends Request {
    private static final long serialVersionUID = 369763071864425931L;
    public NodeAddress peer;
    public NodeAddress sender;

    public RemovePeerRequest(NodeAddress peer) {
        this.peer = peer;
    }
}
