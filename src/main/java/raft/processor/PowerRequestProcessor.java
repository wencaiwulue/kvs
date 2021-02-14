package raft.processor;

import db.core.storage.impl.MapStorage;
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
        node.setStart(!request.isStopService()); // this close the server is soft shutdown, just not provide service, but still running
        if (request.isPowerOff()) {
            if (node.getDb().storage instanceof MapStorage) {
                ((MapStorage<?, ?>) (node.getDb().storage)).writeDataToDisk();
            }
            Runtime.getRuntime().exit(0); // power off
        }
        return new PowerResponse(true);
    }
}
