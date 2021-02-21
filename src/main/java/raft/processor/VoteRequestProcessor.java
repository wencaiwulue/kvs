package raft.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.Node;
import raft.enums.Role;
import rpc.model.requestresponse.Request;
import rpc.model.requestresponse.Response;
import rpc.model.requestresponse.VoteRequest;
import rpc.model.requestresponse.VoteResponse;

/**
 * @author naison
 * @since 4/12/2020 17:56
 */
public class VoteRequestProcessor implements Processor {

    private static final Logger LOGGER = LoggerFactory.getLogger(VoteRequestProcessor.class);

    @Override
    public boolean supports(Request req) {
        return req instanceof VoteRequest;
    }

    @Override
    public Response process(Request req, Node node) {
        node.getWriteLock().lock();
        try {
            VoteRequest request = (VoteRequest) req;
            LOGGER.debug("{} --> {}, vote request info: {}", request.getCandidateId().getPort(), node.getLocalAddress().getPort(), request);
            int i = Long.compare(request.getTerm(), node.getCurrentTerm());
            int j = Long.compare(request.getLastLogTerm(), node.getLogEntries().getLastLogTerm());
            if (j == 0) {
                j = Long.compare(request.getLastLogIndex(), node.getLogEntries().getLastLogIndex());
            }

            // if request term is bigger than current node term
            // or
            // this term don't vote for anyone
            if (i > 0 || (i == 0 && node.getLastVoteFor() == null)) {
                node.setLastVoteFor(request.getCandidateId());
                node.setCurrentTerm(request.getTerm());
                node.setRole(Role.FOLLOWER);
                node.setLeaderAddress(null);
                return new VoteResponse(request.getTerm(), true);
            } else {
                return new VoteResponse(node.getCurrentTerm(), false);
            }
        } finally {
            node.getWriteLock().unlock();
        }
    }
}
