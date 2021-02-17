package raft.processor;

import raft.Node;
import rpc.model.requestresponse.Request;
import rpc.model.requestresponse.Response;
import rpc.model.requestresponse.SynchronizeStateRequest;

public class SynchronizeNodeStateProcessor implements Processor {
    @Override
    public boolean supports(Request req) {
        return req instanceof SynchronizeStateRequest;
    }

    @Override
    public Response process(Request req, Node node) {
        SynchronizeStateRequest request = (SynchronizeStateRequest) req;
        node.setLeaderAddress(request.getLeaderId());
        node.getAllNodeAddresses().addAll(request.getPeers());
        return null;
    }
}
