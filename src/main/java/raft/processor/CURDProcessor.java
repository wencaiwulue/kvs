package raft.processor;

import db.core.StateMachine;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import raft.LogEntry;
import raft.Node;
import raft.NodeAddress;
import rpc.Client;
import rpc.model.requestresponse.*;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author naison
 * @since 4/15/2020 15:40
 */
public class CURDProcessor implements Processor {

    private static final Logger log = LogManager.getLogger(CURDProcessor.class);

    @Override
    public boolean supports(Request req) {
        return req instanceof CURDKVRequest;
    }

    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    @Override
    public Response process(Request req, Node node) {
        node.writeLock.lock();
        try {
            CURDKVRequest request = (CURDKVRequest) req;
            if (!node.isLeader()) {
                return Client.doRequest(node.leaderAddress, req); // redirect to leader
            }

            List<LogEntry> logEntries = Collections.singletonList(new LogEntry(-1, node.currTerm, request.getKey(), ((CURDKVRequest) req).getValue()));
            for (LogEntry log : logEntries) {
                log.setIndex(++node.logdb.lastLogIndex);
            }

            AtomicInteger ai = new AtomicInteger(0);
            for (NodeAddress peerAddress : node.allNodeAddressExcludeMe()) {
                AppendEntriesResponse res = (AppendEntriesResponse) Client.doRequest(peerAddress, new AppendEntriesRequest(logEntries, node.address));
                if (res != null) {
                    if (res.isSuccess()) {
                        ai.addAndGet(1);
                    } else if (res.getTerm() > node.currTerm) {// receive term is bigger than myself, change to follower, discard current request
                        node.leaderAddress = null;
                        node.currTerm = res.getTerm();
                        node.lastVoteFor = null;
                    }
                }
            }

            if (ai.get() >= node.allNodeAddresses.size() / 2D) { // more than half peer already write to log
                StateMachine.apply(logEntries, node);
            }
            return new CURDResponse();
        } finally {
            node.writeLock.unlock();
        }
    }
}
