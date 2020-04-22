package raft.processor;

import raft.Node;
import rpc.model.requestresponse.InstallSnapshotRequest;
import rpc.model.requestresponse.Request;
import rpc.model.requestresponse.Response;

/**
 * @author naison
 * @since 4/13/2020 22:38
 */
public class InstallSnapshotRequestProcessor implements Processor {
    @Override
    public boolean supports(Request req) {
        return req instanceof InstallSnapshotRequest;
    }

    @Override
    public Response process(Request req, Node node) {
        // copy file, apply
        

        return null;
    }
}
