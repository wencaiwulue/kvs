package raft.processor;

import db.core.StateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.LogEntry;
import raft.Node;
import raft.enums.Role;
import rpc.model.requestresponse.*;
import util.CollectionUtil;

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
                if (request.getCommittedIndex() == node.getCommittedIndex()) {
                    node.getLogdb().save(request.getEntries());
                } else if (request.getCommittedIndex() > node.getCommittedIndex()) {
                    long size = request.getCommittedIndex() - request.getPrevLogIndex();
                    // if out of time too much, then use InstallSnapshot to scp zip file and install
                    if (size < 100) {
                        for (long i = 1; i < size + 1; i++) {
                            LogEntry entry = (LogEntry) node.getLogdb().get(String.valueOf(request.getPrevLogIndex() + i));
                            if (entry != null) {
                                StateMachine.writeLogToDB(node, entry);
                            }
                        }
                        node.setCommittedIndex(request.getCommittedIndex());
                    } else {
                        // install snapshot
                        return new ErrorResponse();
                    }
                } else {
                    LOGGER.error("this should not happened");
                }
                return new AppendEntriesResponse(node.getCurrentTerm(), true, node.getLogdb().getLastLogIndex());
            }
        } finally {
            node.getWriteLock().unlock();
        }
    }
}
