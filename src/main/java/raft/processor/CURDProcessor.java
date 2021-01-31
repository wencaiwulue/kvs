package raft.processor;

import db.core.StateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.LogEntry;
import raft.Node;
import raft.NodeAddress;
import raft.enums.CURDOperation;
import rpc.model.requestresponse.*;
import rpc.netty.RpcClient;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author naison
 * @since 4/15/2020 15:40
 */
public class CURDProcessor implements Processor {

    private static final Logger LOGGER = LoggerFactory.getLogger(CURDProcessor.class);

    @Override
    public boolean supports(Request req) {
        return req instanceof CURDKVRequest;
    }

    @Override
    public Response process(Request req, Node node) {
        CURDKVRequest request = (CURDKVRequest) req;

        if (CURDOperation.get.equals(request.getOperation())) { // if it's get operation, get data and return
            Object val = node.getDb().get(request.getKey());
            return new CURDResponse(true, val);
        }

        node.getWriteLock().lock();
        try {
            if (!node.isLeader()) {
                return RpcClient.doRequest(node.getLeaderAddress(), req); // redirect to leader
            }

            List<LogEntry> logEntries = Collections.singletonList(new LogEntry(-1, node.getCurrentTerm(), request.getOperation(), request.getKey(), request.getValue()));
            for (LogEntry log : logEntries) {
                int newValue = node.getLogdb().getLastLogIndex() + 1;
                node.getLogdb().setLastLogIndex(newValue);
                log.setIndex(newValue);
            }

            AtomicInteger ai = new AtomicInteger(0);
            for (NodeAddress peerAddress : node.allNodeAddressExcludeMe()) {
                AppendEntriesResponse res = (AppendEntriesResponse) RpcClient.doRequest(peerAddress, new AppendEntriesRequest(logEntries, node.getLocalAddress(), node.getCurrentTerm(), node.getLastAppliedIndex().intValue(), node.getLastAppliedTerm(), node.getCommittedIndex()));
                if (res != null) {
                    if (res.isSuccess()) {
                        ai.addAndGet(1);
                    } else if (res.getTerm() > node.getCurrentTerm()) {// receive term is bigger than myself, change to follower, discard current request
                        node.setLeaderAddress(null);
                        node.setCurrentTerm(res.getTerm());
                        node.setLastVoteFor(null);
                    }
                }
            }

            if (ai.get() >= node.getAllNodeAddresses().size() / 2D) { // more than half peer already write to log
                StateMachine.apply(logEntries, node);
                return new CURDResponse(true, request.getValue());
            }
            return new CURDResponse(false, null);
        } finally {
            node.getWriteLock().unlock();
        }
    }
}
