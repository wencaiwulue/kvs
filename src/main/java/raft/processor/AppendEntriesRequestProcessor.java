package raft.processor;

import db.core.StateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.LogEntry;
import raft.Node;
import raft.enums.Role;
import rpc.model.requestresponse.*;
import util.CollectionUtil;

import java.util.ArrayList;
import java.util.List;

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
                LOGGER.info("{} --> {}, receive heartbeat, term: {}", request.getLeaderId().getSocketAddress().getPort(), node.getLocalAddress().getSocketAddress().getPort(), request.getTerm());
                switch (node.getRole()) {
                    case LEADER:
                        LOGGER.error("leader receive heartbeats ??");
                        return new AppendEntriesResponse(node.getCurrentTerm(), false, node.getLogdb().getLastLogIndex());
                    case FOLLOWER:
                        if (request.getTerm() < node.getCurrentTerm()) {
                            LOGGER.error("leader term should not less than follower's term");
                            return new AppendEntriesResponse(node.getCurrentTerm(), false, node.getLogdb().getLastLogIndex());
                        } else if (request.getTerm() >= node.getCurrentTerm()) {
                            node.setCurrentTerm(request.getTerm());
                            node.setLeaderAddress(request.getLeaderId());
                            // second round to apply log to db
                            long size = request.getCommittedIndex() - node.getCommittedIndex();
                            if (size > 0) {
                                LOGGER.info("Second round to apply log to db");
                                if (size < 100) {
                                    List<LogEntry> logEntryList = new ArrayList<>();
                                    for (long i = node.getCommittedIndex() + 1; i <= request.getCommittedIndex(); i++) {
                                        LogEntry logEntry = node.getLogdb().get(i);
                                        if (logEntry == null) {
                                            LOGGER.warn("index: {} not found log at node: {}", i, node.getLocalAddress().getSocketAddress().getPort());
                                        } else {
                                            logEntryList.add(logEntry);
                                        }
                                    }
                                    StateMachine.apply(logEntryList, node);
                                } else {
                                    // out of date too much, needs to install snapshot for synchronizing data
                                    return new ErrorResponse();
                                }
                            }
                        }
                        break;
                    case CANDIDATE:
                        if (request.getTerm() < node.getCurrentTerm()) {
                            LOGGER.error("leader term should not less than candidate's term");
                            return new AppendEntriesResponse(node.getCurrentTerm(), false, node.getLogdb().getLastLogIndex());
                        } else if (request.getTerm() >= node.getCurrentTerm()) {
                            node.setCurrentTerm(request.getTerm());
                            node.setLeaderAddress(request.getLeaderId());
                            node.setRole(Role.FOLLOWER);
                        }
                        break;
                }
                return new AppendEntriesResponse(node.getCurrentTerm(), true, node.getLogdb().getLastLogIndex());
            } else {
                LOGGER.info("{} --> {}, receive synchronize log, term: {}", request.getLeaderId().getSocketAddress().getPort(), node.getLocalAddress().getSocketAddress().getPort(), request.getTerm());
                switch (node.getRole()) {
                    case FOLLOWER:
                        if (request.getTerm() > node.getCurrentTerm()) {
                            node.setCurrentTerm(request.getTerm());
                            node.getLogdb().save(request.getEntries());
                        } else if (request.getTerm() == node.getCurrentTerm()) {
                            node.getLogdb().save(request.getEntries());
                        } else {
                            return new AppendEntriesResponse(node.getCurrentTerm(), false, node.getLogdb().getLastLogIndex());
                        }
                        break;
                    case CANDIDATE:
                        if (request.getTerm() > node.getCurrentTerm()) {
                            node.setCurrentTerm(request.getTerm());
                            node.setRole(Role.FOLLOWER);
                            request.getEntries().forEach(e -> node.getDb().set(e.getKey(), e.getValue()));
                        } else if (request.getTerm() == node.getCurrentTerm()) {
                            node.setRole(Role.FOLLOWER);
                            request.getEntries().forEach(e -> node.getDb().set(e.getKey(), e.getValue()));
                        } else {
                            return new AppendEntriesResponse(node.getCurrentTerm(), false, node.getLogdb().getLastLogIndex());
                        }
                        break;
                    case LEADER:
                        if (request.getTerm() > node.getCurrentTerm()) {
                            node.setCurrentTerm(request.getTerm());
                            node.setRole(Role.FOLLOWER);
                            request.getEntries().forEach(e -> node.getDb().set(e.getKey(), e.getValue()));
                        } else if (request.getTerm() == node.getCurrentTerm()) {
                            LOGGER.error("This is impossible");
                            return new AppendEntriesResponse(node.getCurrentTerm(), false, node.getLogdb().getLastLogIndex());
                        } else {
                            return new AppendEntriesResponse(node.getCurrentTerm(), false, node.getLogdb().getLastLogIndex());
                        }
                        break;
                }
                return new AppendEntriesResponse(node.getCurrentTerm(), true, node.getLogdb().getLastLogIndex());
            }
        } finally {
            node.getWriteLock().unlock();
        }
    }
}
