package raft.processor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import raft.Node;
import raft.enums.Role;
import rpc.model.requestresponse.AppendEntriesRequest;
import rpc.model.requestresponse.AppendEntriesResponse;
import rpc.model.requestresponse.Request;
import rpc.model.requestresponse.Response;

/**
 * @author naison
 * @since 4/13/2020 14:42
 */
public class AppendEntriesRequestProcessor implements Processor {
    private static final Logger log = LogManager.getLogger(AppendEntriesRequestProcessor.class);

    @Override
    public boolean supports(Request req) {
        return req instanceof AppendEntriesRequest;
    }

    @Override
    public Response process(Request req, Node node) {
        node.writeLock.lock();
        try {
            AppendEntriesRequest request = (AppendEntriesRequest) req;
            if (request.getTerm() < node.currentTerm) {
                return new AppendEntriesResponse(node.currentTerm, false, node.logdb.lastLogIndex);
            } else if (request.getTerm() > node.currentTerm) {
                node.currentTerm = request.getTerm();
                node.leaderAddress = null;
                node.lastVoteFor = null;
                node.role = Role.FOLLOWER;
            }

            if (!request.getLeaderId().equals(node.leaderAddress)) {
                if (request.getTerm() + 1 > node.currentTerm) {
                    node.currentTerm = request.getTerm() + 1;
                    node.leaderAddress = null;
                    node.lastVoteFor = null;
                    node.role = Role.FOLLOWER;
                }
                return new AppendEntriesResponse(request.getTerm() + 1, false, node.logdb.lastLogIndex);
            }

            if (!request.getEntries().isEmpty()) { // otherwise it's a heartbeat
                node.logdb.save(request.getEntries());
            }
            return new AppendEntriesResponse(node.currentTerm, true, node.logdb.lastLogIndex);
        } finally {
            node.writeLock.unlock();
        }
    }
}
