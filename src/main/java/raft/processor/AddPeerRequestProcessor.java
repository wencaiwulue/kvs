package raft.processor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import raft.Node;
import raft.NodeAddress;
import rpc.RpcClient;
import rpc.model.requestresponse.*;

/**
 * @author naison
 * @since 4/12/2020 17:12
 */
public class AddPeerRequestProcessor implements Processor {

    private static final Logger LOGGER = LogManager.getLogger(AddPeerRequestProcessor.class);

    @Override
    public boolean supports(Request req) {
        return req instanceof AddPeerRequest;
    }

    @Override
    public Response process(Request req, Node node) {
        AddPeerRequest request = (AddPeerRequest) req;

        node.allNodeAddresses.add(request.getPeer());

        if (request.getPeer().equals(request.getSender())) {
            return new AddPeerResponse();// 非主节点，终结 exit 1
        }

        if (!node.isLeader()) {
            if (node.leaderAddress == null) {
                return RpcClient.doRequest(request.getPeer(), new AddPeerRequest(node.address, node.address));
            } else {
                if (request.getPeer() == null) {
                    return RpcClient.doRequest(node.leaderAddress, request);
                } else {
                    return new AddPeerResponse();// exit 2
                }
            }
        } else {
            // leader will notify all node to add new peer,
            // each node receive leader command, will send
            request.sender = node.leaderAddress;
            for (NodeAddress nodeAddress : node.allNodeAddressExcludeMe()) {
                RpcClient.doRequest(nodeAddress, request);
            }
            PowerResponse response = (PowerResponse) RpcClient.doRequest(request.getPeer(), new PowerRequest(false, false));
            if (response != null && response.isSuccess()) {
                return new AddPeerResponse();
            } else {
                return new ErrorResponse("add peer failed, peer info: " + request.getPeer());
            }
        }

    }
}
