package raft.processor;

import db.core.storage.impl.MapStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.Node;
import raft.NodeAddress;
import rpc.model.requestresponse.RemovePeerRequest;
import rpc.model.requestresponse.RemovePeerResponse;
import rpc.model.requestresponse.Request;
import rpc.model.requestresponse.Response;
import rpc.netty.RpcClient;

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

        Runnable r = () -> {
            node.getAllNodeAddresses().remove(request.getPeer());
            // self receive removePeerRequest from leader, shutdown
            if (node.getLocalAddress().equals(request.getPeer())) {
                node.shutdown();
            }
        };

        if (!node.isLeader()) {
            if (node.getLeaderAddress() == null) {
                LOGGER.error("This should not happen");
                return new RemovePeerResponse();
            } else {
                // request from leader
                if (node.getLeaderAddress().equals(request.getSender())) {
                    r.run();
                    return new RemovePeerResponse();// exit 2
                } else {
                    // Redirect to leader
                    return RpcClient.doRequest(node.getLeaderAddress(), request);
                }
            }
        } else {
            // leader will notify all node to remove peer,
            // each node receive leader command, will ask the remove peer to remove itself
            request.setSender(node.getLocalAddress());
            for (NodeAddress nodeAddress : node.allNodeAddressExcludeMe()) {
                RpcClient.doRequestAsync(nodeAddress, request, null);
            }
            r.run();
            return new RemovePeerResponse();
        }
    }
}
