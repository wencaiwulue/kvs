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
public class RemovePeerRequestProcessor implements Processor {

    private static final Logger log = LogManager.getLogger(RemovePeerRequestProcessor.class);

    @Override
    public boolean supports(Request req) {
        return req instanceof RemovePeerRequest;
    }

    @Override
    public Response process(Request req, Node node) {
        RemovePeerRequest request = (RemovePeerRequest) req;
        node.peerAddress.remove(request.getPeer());
        Response response = Client.doRequest(request.getPeer(), new PowerRequest(true, false));
        if (response != null) {
            return new RemovePeerResponse();
        } else {
            return new ErrorResponse(400, "remove peer failed, because don`t exists node: " + request.getPeer());
        }
    }
}
