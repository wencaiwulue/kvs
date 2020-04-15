package raft.processor;

import raft.Node;
import rpc.model.requestresponse.Request;
import rpc.model.requestresponse.Response;

/**
 * @author naison
 * @since 4/12/2020 18:06
 */
public class TryAcceptRequestProcessor implements Processor {
    @Override
    public boolean supports(Request req) {
        return false;
    }

    @Override
    public Response process(Request req, Node node) {
        return null;
    }
}
