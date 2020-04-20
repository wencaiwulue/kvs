package rpc.model.requestresponse;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import raft.NodeAddress;

import java.util.HashSet;
import java.util.Set;

/**
 * @author naison
 * @since 4/12/2020 15:12
 */
@Getter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class AddPeerResponse extends Response {
    private static final long serialVersionUID = 1051441981605736599L;
    private Set<NodeAddress> anotherNode = new HashSet<>();
}
