package rpc.model.requestresponse;

import lombok.*;
import raft.NodeAddress;

import java.util.HashSet;
import java.util.Set;

/**
 * @author naison
 * @since 4/12/2020 15:12
 */
@Getter
@Setter
@NoArgsConstructor
@ToString
public class AddPeerResponse extends Response {
    private static final long serialVersionUID = 1051441981605736599L;
}
