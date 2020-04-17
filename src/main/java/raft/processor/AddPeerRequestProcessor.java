package raft.processor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import raft.Node;
import rpc.Client;
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
        node.getPeerAddress().add(request.getPeer());
        PowerResponse response = (PowerResponse) Client.doRequest(request.getPeer(), new PowerRequest(false, false));
        if (response != null && response.isSuccess()) {
            return new AddPeerResponse();
        } else {
            return new ErrorResponse("add peer failed, peer info: " + request.getPeer());
        }
    }
}
