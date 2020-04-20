package raft.processor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import raft.Node;
import raft.NodeAddress;
import rpc.Client;
import rpc.model.requestresponse.*;

/**
 * @author naison
 * @since 4/12/2020 17:12
 */
public class RemovePeerRequestProcessor implements Processor {

    private static final Logger log = LogManager.getLogger(RemovePeerRequestProcessor.class);

    @Override
    public boolean supports(Request req) {
        return req instanceof RemovePeerRequest;
    }

    @Override
    public Response process(Request req, Node node) {
        RemovePeerRequest request = (RemovePeerRequest) req;
        node.allNodeAddresses.remove(request.peer);

        if (request.peer.equals(request.sender)) {
            return new RemovePeerResponse();// 非主节点，终结 exit 1
        }

        if (!node.isLeader()) {
            if (node.leaderAddress == null) {
                return Client.doRequest(request.peer, new RemovePeerRequest(node.address, node.address));
            } else {
                if (request.sender == null) {
                    return Client.doRequest(node.leaderAddress, request);
                } else {
                    return new RemovePeerResponse();// exit 2
                }
            }
        } else {
            // leader will notify all node to remove peer,
            // each node receive leader command, will ask the remove peer to remove itself
            request.sender = node.leaderAddress;
            for (NodeAddress nodeAddress : node.allNodeAddressExcludeMe()) {
                Client.doRequest(nodeAddress, request);
            }
            PowerResponse response = (PowerResponse) Client.doRequest(request.peer, new PowerRequest(true, true));
            if (response != null && response.isSuccess()) {
                return new AddPeerResponse();
            } else {
                return new ErrorResponse("add peer failed, peer info: " + request.peer);
            }
        }
    }
}
