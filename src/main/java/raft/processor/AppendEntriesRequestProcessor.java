package raft.processor;

import db.core.StateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.LogEntry;
import raft.Node;
import raft.enums.Role;
import rpc.model.requestresponse.*;
import util.ThreadUtil;

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
            LOGGER.info("{} --> {}, receive heartbeat, term: {}", request.getLeaderId().getSocketAddress().getPort(), node.getLocalAddress().getSocketAddress().getPort(), request.getTerm());
            if (request.getTerm() < node.getCurrentTerm()) {
                // start elect, because leader term is less than current nodes term
                node.setNextElectTime(-1);
                node.setRole(Role.CANDIDATE);
                return new AppendEntriesResponse(node.getCurrentTerm(), false, node.getLogdb().getLastLogIndex());
            } else if (request.getTerm() > node.getCurrentTerm()) {
                node.setCurrentTerm(request.getTerm());
                node.setLeaderAddress(request.getLeaderId());
                node.setLastVoteFor(null);
                node.setRole(Role.FOLLOWER);
            }

            if (!request.getLeaderId().equals(node.getLeaderAddress())) {
                if (request.getTerm() + 1 > node.getCurrentTerm()) {
                    node.setCurrentTerm(request.getTerm() + 1);
                    node.setLeaderAddress(null);
                    node.setLastVoteFor(null);
                    node.setRole(Role.FOLLOWER);
                    node.setNextElectTime(-1);// 立即重新选举
                    ThreadUtil.getThreadPool().execute(node.getElectTask());
                }
                return new AppendEntriesResponse(request.getTerm() + 1, false, node.getLogdb().getLastLogIndex());
            }

            if (!request.getEntries().isEmpty()) { // otherwise it's a heartbeat
                node.getLogdb().save(request.getEntries());
            } else {
                if (request.getCommittedIndex() > node.getCommittedIndex()) {
                    long size = request.getCommittedIndex() - request.getPrevLogIndex();
                    // if out of time too much, the use InstallSnapshot to scp zip file and install
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
                }
            }
            return new AppendEntriesResponse(node.getCurrentTerm(), true, node.getLogdb().getLastLogIndex());
        } finally {
            node.getWriteLock().unlock();
        }
    }
}
