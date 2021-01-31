package rpc.model.requestresponse;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import raft.NodeAddress;

/**
 * @author naison
 * @since 4/14/2020 10:52
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class InstallSnapshotRequest extends Request {
    private static final long serialVersionUID = -8137679486480941058L;

    private NodeAddress leader;
    private int term;
    private String filename;
    private long fileSize;
}
