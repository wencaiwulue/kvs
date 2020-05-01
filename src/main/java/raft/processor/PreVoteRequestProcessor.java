package raft.processor;

import raft.Node;
import rpc.model.requestresponse.PreVoteRequest;
import rpc.model.requestresponse.Request;
import rpc.model.requestresponse.Response;

/**
 * @author naison
 * @since 5/1/2020 14:00
 */
public class PreVoteRequestProcessor implements Processor {
    @Override
    public boolean supports(Request req) {
        return req instanceof PreVoteRequest;
    }

    @Override
    public Response process(Request req, Node node) {
        PreVoteRequest request = (PreVoteRequest) req;
        return null;
    }
}
