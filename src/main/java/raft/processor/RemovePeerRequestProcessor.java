package raft.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.Node;
import raft.NodeAddress;
import rpc.model.requestresponse.*;
import rpc.netty.RpcClient;
import util.ThreadUtil;

/**
 * @author naison
 * @since 4/12/2020 17:12
 */
public class RemovePeerRequestProcessor implements Processor {

    private static final Logger LOGGER = LoggerFactory.getLogger(RemovePeerRequestProcessor.class);

    @Override
    public boolean supports(Request req) {
        return req instanceof RemovePeerRequest;
    }

    @Override
    public Response process(Request req, Node node) {
        RemovePeerRequest request = (RemovePeerRequest) req;
        node.getAllNodeAddresses().remove(request.peer);

        if (!node.isLeader()) {
            if (node.getLeaderAddress() == null) {
                return new RemovePeerResponse();
            } else {
                if (node.getLeaderAddress().equals(request.getSender())) {
                    return new RemovePeerResponse();// exit 2
                } else {
                    return RpcClient.doRequest(node.getLeaderAddress(), request);
                }
            }
        } else {
            // leader will notify all node to remove peer,
            // each node receive leader command, will ask the remove peer to remove itself
            request.sender = node.getLeaderAddress();
            for (NodeAddress nodeAddress : node.allNodeAddressExcludeMe()) {
                ThreadUtil.getThreadPool().execute(() -> RpcClient.doRequest(nodeAddress, request));
            }
            PowerResponse response = (PowerResponse) RpcClient.doRequest(request.peer, new PowerRequest(true, true));
            if (response != null && response.isSuccess()) {
                return new RemovePeerResponse();
            } else {
                return new ErrorResponse("remove peer failed, peer info: " + request.peer);
            }
        }
    }
}
