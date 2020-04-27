package raft.processor;

import raft.Node;
import rpc.model.requestresponse.PowerRequest;
import rpc.model.requestresponse.PowerResponse;
import rpc.model.requestresponse.Request;
import rpc.model.requestresponse.Response;

/**
 * @author naison
 * @since 4/14/2020 09:26
 */
public class PowerRequestProcessor implements Processor {
    @Override
    public boolean supports(Request req) {
        return req instanceof PowerRequest;
    }

    @Override
    public Response process(Request req, Node node) {
        PowerRequest request = (PowerRequest) req;
        node.start = !request.stopService; // this close the server is soft shutdown, just not provide service, but still running
        if (request.powerOff) {
            node.db.writeDataToDisk();
            node.logdb.writeDataToDisk();
            Runtime.getRuntime().exit(0); // power off
        }
        return new PowerResponse(true);
    }
}
