package raft.processor;

import db.core.StateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.LogEntry;
import raft.Node;
import raft.enums.Role;
import rpc.model.requestresponse.AppendEntriesRequest;
import rpc.model.requestresponse.AppendEntriesResponse;
import rpc.model.requestresponse.ErrorResponse;
import rpc.model.requestresponse.Request;
import rpc.model.requestresponse.Response;
import util.CollectionUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

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
            // heartbeat
            if (CollectionUtil.isEmpty(request.getEntries())) {
                LOGGER.info("{} --> {}, receive heartbeat, term: {}",
                        request.getLeaderId().getPort(), node.getLocalAddress().getPort(), request.getTerm());
                switch (node.getRole()) {
                    case LEADER:
                        LOGGER.error("leader receive heartbeats ?");
                        return new AppendEntriesResponse(node.getCurrentTerm(), false);
                    case FOLLOWER:
                        if (request.getTerm() < node.getCurrentTerm()) {
                            LOGGER.error("leader term should not less than follower's term");
                            return new AppendEntriesResponse(node.getCurrentTerm(), false);
                        } else if (request.getTerm() >= node.getCurrentTerm()) {
                            node.setCurrentTerm(request.getTerm());
                            node.setLeaderAddress(request.getLeaderId());
                            // Second round, followers apply log to statemachine
                            long size = request.getLeaderCommit() - node.getCommittedIndex();
                            if (size > 0) {
                                LOGGER.info("Second round to apply log to db");
                                if (size < 100) {
                                    List<LogEntry> logEntryList = new ArrayList<>();
                                    for (long i = node.getCommittedIndex() + 1; i <= request.getLeaderCommit(); i++) {
                                        LogEntry logEntry = node.getLogEntries().get(i);
                                        if (logEntry == null) {
                                            LOGGER.warn("index: {} not found log at node: {}",
                                                    i, node.getLocalAddress().getPort());
                                        } else {
                                            logEntryList.add(logEntry);
                                        }
                                    }
                                    StateMachine.apply(logEntryList, node);
                                    return new AppendEntriesResponse(node.getCurrentTerm(), true);
                                } else {
                                    // out of date too much, needs to install snapshot for synchronizing log
                                    return new ErrorResponse();
                                }
                            } else if (size < 0) {
                                LOGGER.error("Leader commitIndex < follower commitIndex, this is impossible");
                            }
                        }
                        return new AppendEntriesResponse(node.getCurrentTerm(), true);
                    case CANDIDATE:
                        if (request.getTerm() < node.getCurrentTerm()) {
                            LOGGER.error("leader term should not less than candidate's term");
                            return new AppendEntriesResponse(node.getCurrentTerm(), false);
                        } else if (request.getTerm() >= node.getCurrentTerm()) {
                            node.setCurrentTerm(request.getTerm());
                            node.setLeaderAddress(request.getLeaderId());
                            node.setRole(Role.FOLLOWER);
                            return new AppendEntriesResponse(node.getCurrentTerm(), true);
                        }
                        break;
                    default:
                        return new AppendEntriesResponse(node.getCurrentTerm(), false);
                }
                return new AppendEntriesResponse(node.getCurrentTerm(), true);
            } else {
                // First round, followers append log
                LOGGER.info("{} --> {}, Receive synchronize log, term: {}",
                        request.getLeaderId().getPort(), node.getLocalAddress().getPort(), request.getTerm());
                // Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
                Function<AppendEntriesRequest, Boolean> preCheckOk = e -> Optional
                        .ofNullable(node.getLogEntries().get(e.getPrevLogIndex()))
                        .filter(o -> o.getTerm() == e.getPrevLogTerm())
                        .isPresent();
                BiConsumer<AppendEntriesRequest, Node> consumeIfOk = (pReq, pNode) -> {
                    // If an existing entry conflicts with a new one (same index but different terms),
                    // delete the existing entry and all that follow it
                    int firstNotMatch = 0;
                    for (int i = 0; i < pReq.getEntries().size(); i++) {
                        LogEntry entry = pReq.getEntries().get(i);
                        if (pNode.getLogEntries().get(entry.getIndex()) != null) {
                            firstNotMatch = i;
                            long indexExclusive = pNode.getLogEntries().getLastLogIndex() + 1;
                            pNode.getLogEntries().removeRange(entry.getIndex(), indexExclusive);
                            break;
                        }
                    }
                    // Append any new entries not already in the log
                    List<LogEntry> temp = new ArrayList<>();
                    pReq.getEntries().listIterator(firstNotMatch).forEachRemaining(temp::add);
                    pNode.getLogEntries().save(temp);
                    // If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
                    if (pReq.getLeaderCommit() > pNode.getCommittedIndex()) {
                        pNode.setCommittedIndex(Math.min(pReq.getLeaderCommit(), pNode.getLogEntries().getLastLogIndex()));
                    }
                    LOGGER.info("Append log successfully");
                };
                BiFunction<AppendEntriesRequest, Node, Boolean> func = (p1, p2) -> {
                    Boolean isOk = preCheckOk.apply(p1);
                    // TODO
                    if (/*isOk*/true) {
                        consumeIfOk.accept(p1, p2);
                    } else {
                        LOGGER.warn("PreCheck not pass !");
                    }
                    return /*isOk*/true;
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
