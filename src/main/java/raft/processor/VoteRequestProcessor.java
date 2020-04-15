package raft.processor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import raft.Node;
import raft.enums.State;
import rpc.model.requestresponse.Request;
import rpc.model.requestresponse.Response;
import rpc.model.requestresponse.VoteRequest;
import rpc.model.requestresponse.VoteResponse;

/**
 * @author naison
 * @since 4/12/2020 17:56
 */
public class VoteRequestProcessor implements Processor {

    private static final Logger log = LogManager.getLogger(VoteRequestProcessor.class);

    @Override
    public boolean supports(Request req) {
        return req instanceof VoteRequest;
    }

    @Override
    public Response process(Request req, Node node) {
        node.getWriteLock().lock();
        try {
            VoteRequest request = (VoteRequest) req;
            log.error("收到vote请求.from:{} to {}, vote info:{}", request.getCandidateId(), node.getAddress(), request);
            int i = Long.compare(request.getLastLogTerm(), node.logdb.lastLogTerm);
//            int j = Long.compare(request.getLastLogIndex(), node.logdb.lastLogIndex);
            if (i > 0 || (i == 0 && node.lastVoteFor == null)) {// 对方term比我大，或者还没投过票
                node.lastVoteFor = request.getCandidateId();
                node.currTerm = request.getTerm();
                node.leaderAddr = null;
                node.state = State.FOLLOWER;
                return new VoteResponse(request.getTerm(), true);
            } else {
                return new VoteResponse(node.currTerm, false);
            }
        } finally {
            node.getWriteLock().unlock();
        }
    }
}
