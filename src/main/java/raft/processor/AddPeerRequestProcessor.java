package raft.processor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import raft.Node;
import rpc.model.requestresponse.*;

/**
 * @author naison
 * @since 4/12/2020 17:12
 */
public class AddPeerRequestProcessor implements Processor {

    private static final Logger log = LogManager.getLogger(AddPeerRequestProcessor.class);

    @Override
    public boolean supports(Request req) {
        return req instanceof AddPeerRequest;
    }

    @Override
    public Response process(Request req, Node node) {
        AddPeerRequest request = (AddPeerRequest) req;
        boolean add = node.getPeerAddress().add(request.getPeer());
        Response response;
        if (add) {
            response = new AddPeerResponse();
        } else {
            response = new ErrorResponse(400, "add peer failed, because already exists node: " + request.getPeer());
        }
        return response;
    }
}
