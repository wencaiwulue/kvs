package raft.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.LogEntry;
import raft.Node;
import raft.NodeAddress;
import raft.enums.CURDOperation;
import raft.enums.Role;
import rpc.model.requestresponse.AppendEntriesRequest;
import rpc.model.requestresponse.AppendEntriesResponse;
import rpc.model.requestresponse.CURDRequest;
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
        return req instanceof CURDRequest;
    }

    /**
     * 1, leader add log, but not push commitIndex
     * 2, leader send first time of appendEntities, to notify peer write log
     * 3, if (most peer write log successfully)
     * -    leader apply log to statemachine, push commitIndex, notify peer to apply log to statemachine
     * -  else
     * -    will not apply log, not push commitIndex, not notify peer
     * 4, if most peer apply log to statemachine, return result to client
     *
     * @param req  req
     * @param node node
     * @return result
     */
    @Override
    public Response process(Request req, Node node) {
        CURDRequest request = (CURDRequest) req;

        if (CURDOperation.get.equals(request.getOperation())) { // if it's get operation, get data and return
            Object val = node.getStateMachine().get(request.getKey()[0]);
            LOGGER.info("get key: {} from db and value: {}", request.getKey(), val);
            return new CURDResponse(true, new Object[]{val});
        } else if (CURDOperation.mget.equals(request.getOperation())) {
            Object[] values = new Object[request.getKey().length];
            for (int i = 0; i < request.getKey().length; i++) {
                values[i] = node.getStateMachine().get(request.getKey()[i]);
            }
            return new CURDResponse(true, values);
        }
        if (!node.isLeader()) {
            LOGGER.info("Redirect to leader: {},  current node: {}, role: {}", node.getLeaderAddress().getPort(), node.getLocalAddress().getPort(), node.getRole());
            return node.getRpcClient().doRequest(node.getLeaderAddress(), req); // redirect to leader
        }

        node.getWriteLock().lock();
        try {
            LOGGER.info("Leader receive CURD request");

            List<LogEntry> logEntries = new ArrayList<>(request.getKey().length);
            long lastValue = node.getLogEntries().getLastLogIndex();
            for (int i = 0; i < request.getKey().length; i++) {
                lastValue += 1;
                LogEntry logEntry = new LogEntry(lastValue, node.getCurrentTerm(), request.getOperation(), request.getKey()[i], request.getValue()[i]);
                logEntries.add(logEntry);
            }
            // leader append log
            node.getLogEntries().save(logEntries);

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
                AppendEntriesRequest entriesRequest = AppendEntriesRequest.builder()
                        .term(node.getCurrentTerm())
                        .leaderId(node.getLocalAddress())
                        .prevLogIndex(node.getLastAppliedIndex())
                        .prevLogTerm(node.getLogEntries().getLastLogTerm())
                        .entries(logEntries)
                        .leaderCommit(node.getCommittedIndex())
                        .build();
                requestIds.add(entriesRequest.getRequestId());
                // First round, leader notify peers to append log
                node.getRpcClient().doRequestAsync(peerAddress, entriesRequest, c);
            }
            try {
                boolean await = latch.await(1000, TimeUnit.MILLISECONDS);
                if (!await) {
                    LOGGER.warn("Waiting for appending entries response timeout");
                    requestIds.forEach(e -> node.getRpcClient().cancelRequest(e, true));
                    return new CURDResponse(false, null);
                }
            } catch (InterruptedException ex) {
                requestIds.forEach(e -> node.getRpcClient().cancelRequest(e, true));
                LOGGER.warn("Waiting for response was interrupted, info: {}", ex.getMessage());
                return new CURDResponse(false, null);
            }

            // more than half peer already write to log
            if (ai.get() >= (node.getAllNodeAddresses().size() / 2 + 1)) {
                // Second round, leader notify peers to apply log to statemachine
                node.applyLogToStatemachine(logEntries);
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
