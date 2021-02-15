package raft.processor;

import db.core.StateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.LogEntry;
import raft.Node;
import raft.NodeAddress;
import raft.enums.CURDOperation;
import raft.enums.Role;
import rpc.model.requestresponse.AppendEntriesRequest;
import rpc.model.requestresponse.AppendEntriesResponse;
import rpc.model.requestresponse.CURDKVRequest;
import rpc.model.requestresponse.CURDResponse;
import rpc.model.requestresponse.Request;
import rpc.model.requestresponse.Response;
import rpc.netty.RpcClient;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

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

    /**
     * 1, leader add log, but not push commitIndex
     * 2, leader send first time of appendEntities, to notify peer write log
     * 3, if (most peer write log successfully)
     * -    leader apply log to db, push commitIndex, notify peer to apply log to db
     * -  else
     * -    will not apply log, not push commitIndex, not notify peer
     * 4, if most peer apply log to db, return result to client
     *
     * @param req  req
     * @param node node
     * @return result
     */
    @Override
    public Response process(Request req, Node node) {
        CURDKVRequest request = (CURDKVRequest) req;

        if (CURDOperation.get.equals(request.getOperation())) { // if it's get operation, get data and return
            Object val = node.getDb().get(request.getKey()[0]);
            LOGGER.info("get key: {} from db and value: {}", request.getKey(), val);
            return new CURDResponse(true, new Object[]{val});
        } else if (CURDOperation.mget.equals(request.getOperation())) {
            Object[] values = new Object[request.getKey().length];
            for (int i = 0; i < request.getKey().length; i++) {
                values[i] = node.getDb().get(request.getKey()[i]);
            }
            return new CURDResponse(true, values);
        }

        node.getWriteLock().lock();
        try {
            if (!node.isLeader()) {
                LOGGER.warn("Redirect to leader: {},  current node: {}, role: {}", node.getLeaderAddress().getSocketAddress().getPort(), node.getLocalAddress().getSocketAddress().getPort(), node.getRole());
                return RpcClient.doRequest(node.getLeaderAddress(), req); // redirect to leader
            }
            LOGGER.info("Leader receive CURD request");

            List<LogEntry> logEntries = new ArrayList<>(request.getKey().length);
            int lastValue = node.getLogdb().getLastLogIndex();
            for (int i = 0; i < request.getKey().length; i++) {
                lastValue += 1;
                LogEntry logEntry = new LogEntry(lastValue, node.getCurrentTerm(), request.getOperation(), request.getKey()[i], request.getValue()[i]);
                logEntries.add(logEntry);
            }
            node.getLogdb().setLastLogIndex(lastValue);
            // leader add log
            node.getLogdb().save(logEntries);

            AtomicInteger ai = new AtomicInteger(1);
            int size = node.allNodeAddressExcludeMe().size();
            CountDownLatch latch = new CountDownLatch(size);
            List<Integer> requestIds = new ArrayList<>(size);
            for (NodeAddress peerAddress : node.allNodeAddressExcludeMe()) {
                Consumer<Response> c = res -> {
                    try {
                        AppendEntriesResponse response = (AppendEntriesResponse) res;
                        if (response != null) {
                            if (response.isSuccess()) {
                                ai.addAndGet(1);
                            } else if (response.getTerm() > node.getCurrentTerm()) {// receive term is bigger than myself, change to follower, discard current request
                                node.setLeaderAddress(null);
                                node.setCurrentTerm(response.getTerm());
                                node.setLastVoteFor(null);
                                node.setRole(Role.FOLLOWER);
                            } else if (response.getTerm() == node.getCurrentTerm()) {
//                        node.setCommittedIndex(lastValue);
                            }
                        } else {
                            LOGGER.warn("AppendEntriesResponse is null, attention here");
                        }
                    } finally {
                        latch.countDown();
                    }
                };
                AppendEntriesRequest entriesRequest = new AppendEntriesRequest(node.getCurrentTerm(), node.getLocalAddress(), node.getLastAppliedIndex().intValue(), node.getLogdb().getLastLogTerm(), logEntries, node.getCommittedIndex());
                requestIds.add(entriesRequest.getRequestId());
                RpcClient.doRequestAsync(peerAddress, entriesRequest, c);
            }
            try {
                boolean await = latch.await(1000, TimeUnit.MILLISECONDS);
                if (!await) {
                    LOGGER.warn("Waiting for appending entries response timeout");
                    requestIds.forEach(e -> RpcClient.cancelRequest(e, true));
                    return new CURDResponse(false, null);
                }
            } catch (InterruptedException ex) {
                requestIds.forEach(e -> RpcClient.cancelRequest(e, true));
                LOGGER.warn("Waiting for response was interrupted, info: {}", ex.getMessage());
                return new CURDResponse(false, null);
            }

            if (ai.get() >= (node.getAllNodeAddresses().size() / 2 + 1)) { // more than half peer already write to log
                StateMachine.apply(logEntries, node);
                LOGGER.info("AppendEntries received most peer response, applying data to state machine");
                return new CURDResponse(true, null);
            } else {
                LOGGER.warn("AppendEntries not receive most peer response");
                return new CURDResponse(false, null);
            }
        } finally {
            node.getWriteLock().unlock();
        }
    }
}
