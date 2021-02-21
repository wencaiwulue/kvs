package raft.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.LogEntry;
import raft.Node;
import rpc.model.requestresponse.InstallSnapshotRequest;
import rpc.model.requestresponse.InstallSnapshotResponse;
import rpc.model.requestresponse.Request;
import rpc.model.requestresponse.Response;

import java.util.Arrays;

/**
 * @author naison
 * @since 4/13/2020 22:38
 */
public class InstallSnapshotRequestProcessor implements Processor {

    private static final Logger LOGGER = LoggerFactory.getLogger(InstallSnapshotRequestProcessor.class);

    @Override
    public boolean supports(Request req) {
        return req instanceof InstallSnapshotRequest;
    }

    @Override
    public Response process(Request req, Node node) {
        InstallSnapshotRequest request = (InstallSnapshotRequest) req;
        if (request.getTerm() < node.getCurrentTerm()) {
            return new InstallSnapshotResponse(node.getCurrentTerm());
        }
        if (request.getOffset() == 0) {

        }
        node.getLogEntries().save(Arrays.asList(request.getData()));

        if (!request.isDone()) {
            return new InstallSnapshotResponse(node.getCurrentTerm());
        }

        node.getStateMachine().applyLog(node.getLogEntries().getRange(0, node.getLogEntries().getLastLogIndex() + 1));

        return new InstallSnapshotResponse(node.getCurrentTerm());
    }
}
