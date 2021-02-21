package raft.processor;

import db.core.StateMachine;
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.LogEntry;
import raft.Node;
import raft.enums.Role;
import rpc.model.requestresponse.AppendEntriesRequest;
import rpc.model.requestresponse.AppendEntriesResponse;
import rpc.model.requestresponse.Request;
import rpc.model.requestresponse.Response;
import util.CollectionUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;

/**
 * @author naison
 * @since 4/13/2020 14:42
 */
public class AppendEntriesRequestProcessor implements Processor {
    private static final Logger LOGGER = LoggerFactory.getLogger(AppendEntriesRequestProcessor.class);

    @Override
    public boolean supports(Request req) {
        return req instanceof AppendEntriesRequest;
    }

    @Override
    public Response process(Request req, Node node) {
        node.getWriteLock().lock();
        try {
            // push off elect
            node.setNextElectTime(node.nextElectTime());
            AppendEntriesRequest request = (AppendEntriesRequest) req;
            BiPredicate<AppendEntriesRequest, Node> checkPreEntry = (pRequest, pNode) -> Optional
                    .ofNullable(pNode.getLogEntries().get(pRequest.getPrevLogIndex()))
                    .filter(entry -> entry.getTerm() == pRequest.getPrevLogTerm())
                    .isPresent();
            // heartbeat
            if (CollectionUtil.isEmpty(request.getEntries())) {
                LOGGER.debug("{} --> {}, receive heartbeat, term: {}",
                        request.getLeaderId().getPort(), node.getLocalAddress().getPort(), request.getTerm());
                switch (node.getRole()) {
                    case LEADER:
                        LOGGER.error("Leader receive heartbeats ?");
                        return new AppendEntriesResponse(node.getCurrentTerm(), false);
                    case FOLLOWER:
                        if (request.getTerm() < node.getCurrentTerm()) {
                            LOGGER.error("Leader term should not less than follower's term");
                            return new AppendEntriesResponse(node.getCurrentTerm(), false);
                        } else if (request.getTerm() > node.getCurrentTerm()) {
                            node.setCurrentTerm(request.getTerm());
                            node.setLeaderAddress(request.getLeaderId());
                            return new AppendEntriesResponse(node.getCurrentTerm(), checkPreEntry.test(request, node));
                        } else {
                            node.setLeaderAddress(request.getLeaderId());
                            return new AppendEntriesResponse(node.getCurrentTerm(), checkPreEntry.test(request, node));
                        }
                    case CANDIDATE:
                        if (request.getTerm() < node.getCurrentTerm()) {
                            LOGGER.error("leader term should not less than candidate's term");
                            return new AppendEntriesResponse(node.getCurrentTerm(), false);
                        } else if (request.getTerm() > node.getCurrentTerm()) {
                            node.setCurrentTerm(request.getTerm());
                            node.setLeaderAddress(request.getLeaderId());
                            node.setRole(Role.FOLLOWER);
                            return new AppendEntriesResponse(node.getCurrentTerm(), checkPreEntry.test(request, node));
                        } else {
                            node.setLeaderAddress(request.getLeaderId());
                            node.setRole(Role.FOLLOWER);
                            return new AppendEntriesResponse(node.getCurrentTerm(), checkPreEntry.test(request, node));
                        }
                    default:
                        return new AppendEntriesResponse(node.getCurrentTerm(), false);
                }
            } else {
                BiConsumer<AppendEntriesRequest, Node> consumeIfOk = (pRequest, pNode) -> {
                    // If an existing entry conflicts with a new one (same index but different terms),
                    // delete the existing entry and all that follow it
                    int firstNotMatch = 0;
                    for (int i = 0; i < pRequest.getEntries().size(); i++) {
                        LogEntry entry = pRequest.getEntries().get(i);
                        if (pNode.getLogEntries().get(entry.getIndex()) != null) {
                            firstNotMatch = i;
                            long indexExclusive = pNode.getLogEntries().getLastLogIndex() + 1;
                            pNode.getLogEntries().removeRange(entry.getIndex(), indexExclusive);
                            break;
                        }
                    }
                    // Append any new entries not already in the log
                    List<LogEntry> temp = new ArrayList<>();
                    pRequest.getEntries().listIterator(firstNotMatch).forEachRemaining(temp::add);
                    pNode.getLogEntries().save(temp);
                    // If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
                    if (pRequest.getLeaderCommit() > pNode.getCommittedIndex()) {
                        pNode.setCommittedIndex(Math.min(pRequest.getLeaderCommit(), pNode.getLogEntries().getLastLogIndex()));
                    }
                    LOGGER.info("Append log successfully");
                };
                // First round, followers append log
                BiFunction<AppendEntriesRequest, Node, Boolean> firstRoundFunc = (pRequest, pNode) -> {
                    // Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
                    boolean pass = checkPreEntry.test(pRequest, pNode);
                    // TODO
                    if (/*pass*/true) {
                        consumeIfOk.accept(pRequest, pNode);
                    } else {
                        LOGGER.warn("PreCheck not pass, request: {}", pRequest);
                    }
                    return /*pass*/true;
                };

                // Second round, followers apply log to statemachine
                BiFunction<AppendEntriesRequest, Node, Boolean> secondRoundFunc = (pRequest, pNode) -> {
                    long size = pRequest.getLeaderCommit() - pNode.getCommittedIndex();
                    Assertions.assertTrue(size > 0, "Here error");

                    if (size < 100) {
                        List<LogEntry> logEntryList = new ArrayList<>();
                        for (long i = pNode.getCommittedIndex() + 1; i <= pRequest.getLeaderCommit(); i++) {
                            LogEntry logEntry = pNode.getLogEntries().get(i);
                            if (logEntry == null) {
                                LOGGER.warn("index: {} not found log at node: {}", i, pNode.getLocalAddress().getPort());
                            } else {
                                logEntryList.add(logEntry);
                            }
                        }
                        StateMachine.apply(logEntryList, pNode);
                        return true;
                    } else {
                        // out of date too much, needs to install snapshot for synchronizing log
                        return false;
                    }
                };
                BiFunction<AppendEntriesRequest, Node, Boolean> func = (pRequest, pNode) -> {
                    if (pRequest.getLeaderCommit() == pNode.getCommittedIndex()) {
                        LOGGER.info("{} --> {}, First round, receive synchronize log", request.getLeaderId().getPort(), node.getLocalAddress().getPort());
                        return firstRoundFunc.apply(pRequest, pNode);
                    } else if (pRequest.getLeaderCommit() > pNode.getCommittedIndex()) {
                        LOGGER.info("{} --> {}, Second round, apply log to statemachine", request.getLeaderId().getPort(), node.getLocalAddress().getPort());
                        return secondRoundFunc.apply(pRequest, pNode);
                    } else {
                        LOGGER.warn("{} --> {}, AppendEntries request leader's commit index < follower's commit index, this should not happened", request.getLeaderId().getPort(), node.getLocalAddress().getPort());
                        return false;
                    }
                };
                switch (node.getRole()) {
                    case FOLLOWER:
                        if (request.getTerm() > node.getCurrentTerm()) {
                            node.setCurrentTerm(request.getTerm());
                            return new AppendEntriesResponse(node.getCurrentTerm(), func.apply(request, node));
                        } else if (request.getTerm() == node.getCurrentTerm()) {
                            return new AppendEntriesResponse(node.getCurrentTerm(), func.apply(request, node));
                        } else {
                            return new AppendEntriesResponse(node.getCurrentTerm(), false);
                        }
                    case CANDIDATE:
                        if (request.getTerm() > node.getCurrentTerm()) {
                            node.setCurrentTerm(request.getTerm());
                            node.setRole(Role.FOLLOWER);
                            return new AppendEntriesResponse(node.getCurrentTerm(), func.apply(request, node));
                        } else if (request.getTerm() == node.getCurrentTerm()) {
                            node.setRole(Role.FOLLOWER);
                            return new AppendEntriesResponse(node.getCurrentTerm(), func.apply(request, node));
                        } else {
                            return new AppendEntriesResponse(node.getCurrentTerm(), false);
                        }
                    case LEADER:
                        if (request.getTerm() > node.getCurrentTerm()) {
                            node.setCurrentTerm(request.getTerm());
                            node.setRole(Role.FOLLOWER);
                            return new AppendEntriesResponse(node.getCurrentTerm(), func.apply(request, node));
                        } else if (request.getTerm() == node.getCurrentTerm()) {
                            LOGGER.error("This is impossible");
                            return new AppendEntriesResponse(node.getCurrentTerm(), false);
                        } else {
                            return new AppendEntriesResponse(node.getCurrentTerm(), false);
                        }
                    default:
                        return new AppendEntriesResponse(node.getCurrentTerm(), false);
                }
            }
        } finally {
            node.getWriteLock().unlock();
        }
    }
}
