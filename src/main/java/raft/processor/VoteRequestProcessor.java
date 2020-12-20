package raft.processor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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

    private static final Logger LOGGER = LogManager.getLogger(VoteRequestProcessor.class);

    @Override
    public boolean supports(Request req) {
        return req instanceof VoteRequest;
    }

    @Override
    public Response process(Request req, Node node) {
        node.getWriteLock().lock();
        try {
            VoteRequest request = (VoteRequest) req;
            LOGGER.error("收到vote请求.from:{} to {}, vote info:{}", request.getCandidateId().getSocketAddress().getPort(), node.getAddress().getSocketAddress().getPort(), request);
            int i = Long.compare(request.getTerm(), node.currentTerm);

            int j = Long.compare(request.getLastLogTerm(), node.logdb.lastLogTerm);
            if (j == 0) j = Long.compare(request.getLastLogIndex(), node.logdb.lastLogIndex);

            if (i > 0 || (j >= 0 && node.lastVoteFor == null)) {// 对方term比我大，或者还没投过票
                node.lastVoteFor = request.getCandidateId();
                node.currentTerm = request.getTerm();
                node.leaderAddress = null;
                node.role = Role.FOLLOWER;
                return new VoteResponse(request.getTerm(), true);
            } else {
                return new VoteResponse(node.currentTerm, false);
            }
        } finally {
            node.getWriteLock().unlock();
        }
    }
}
