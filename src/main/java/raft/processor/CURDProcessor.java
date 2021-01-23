package raft.processor;

import db.core.StateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.LogEntry;
import raft.Node;
import raft.NodeAddress;
import raft.enums.CURDOperation;
import rpc.model.requestresponse.*;
import rpc.netty.pub.RpcClient;

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

    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    @Override
    public Response process(Request req, Node node) {
        CURDKVRequest request = (CURDKVRequest) req;

        if (CURDOperation.get.equals(request.getOperation())) { // if it's get operation, get data and return
            Object val = node.db.get(request.getKey());
            return new CURDResponse(true, val);
        }

        node.writeLock.lock();
        try {
            if (!node.isLeader()) {
                return RpcClient.doRequest(node.leaderAddress, req); // redirect to leader
            }

            List<LogEntry> logEntries = Collections.singletonList(new LogEntry(-1, node.currentTerm, request.getOperation(), request.getKey(), request.getValue()));
            for (LogEntry log : logEntries) {
                log.index = ++node.logdb.lastLogIndex;
            }

            AtomicInteger ai = new AtomicInteger(0);
            for (NodeAddress peerAddress : node.allNodeAddressExcludeMe()) {
                AppendEntriesResponse res = (AppendEntriesResponse) RpcClient.doRequest(peerAddress, new AppendEntriesRequest(logEntries, node.address, node.currentTerm, node.getLastAppliedIndex().intValue(), node.getLastAppliedTerm(), node.committedIndex));
                if (res != null) {
                    if (res.isSuccess()) {
                        ai.addAndGet(1);
                    } else if (res.getTerm() > node.currentTerm) {// receive term is bigger than myself, change to follower, discard current request
                        node.leaderAddress = null;
                        node.currentTerm = res.getTerm();
                        node.lastVoteFor = null;
                    }
                }
            }

            if (ai.get() >= node.allNodeAddresses.size() / 2D) { // more than half peer already write to log
                StateMachine.apply(logEntries, node);
                return new CURDResponse(true, request.getValue());
            }
            return new CURDResponse(false, null);
        } finally {
            node.writeLock.unlock();
        }
    }
}
