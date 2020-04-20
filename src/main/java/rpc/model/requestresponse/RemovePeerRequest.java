package rpc.model.requestresponse;

import lombok.AllArgsConstructor;
import lombok.Data;
import raft.NodeAddress;

/**
 * @author naison
 * @since 4/12/2020 15:13
 */
@AllArgsConstructor
public class RemovePeerRequest extends Request {
    private static final long serialVersionUID = 369763071864425931L;
    public NodeAddress peer;
    public NodeAddress sender;
}
