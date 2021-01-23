package raft.processor;

import db.core.StateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.LogEntry;
import raft.Node;
import raft.enums.Role;
import rpc.model.requestresponse.*;
import rpc.netty.server.WebSocketServer;
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
        node.writeLock.lock();
        try {
            // push off elect
            node.nextElectTime = node.nextElectTime();
            AppendEntriesRequest request = (AppendEntriesRequest) req;
            LOGGER.error("{} --> {}, receive heartbeat, term: {}", request.getLeaderId().getSocketAddress().getPort(), WebSocketServer.PORT, request.getTerm());
            if (request.getTerm() < node.currentTerm) {
                // start elect, because leader term is less than current nodes term
                node.nextElectTime = -1;
                node.role = Role.CANDIDATE;
                return new AppendEntriesResponse(node.currentTerm, false, node.logdb.lastLogIndex);
            } else if (request.getTerm() > node.currentTerm) {
                node.currentTerm = request.getTerm();
                node.leaderAddress = request.getLeaderId();
                node.lastVoteFor = null;
                node.role = Role.FOLLOWER;
            }

            if (!request.getLeaderId().equals(node.leaderAddress)) {
                if (request.getTerm() + 1 > node.currentTerm) {
                    node.currentTerm = request.getTerm() + 1;
                    node.leaderAddress = null;
                    node.lastVoteFor = null;
                    node.role = Role.FOLLOWER;
                    node.nextElectTime = -1;// 立即重新选举
                    ThreadUtil.getThreadPool().execute(node.electTask);
                }
                return new AppendEntriesResponse(request.getTerm() + 1, false, node.logdb.lastLogIndex);
            }

            if (!request.getEntries().isEmpty()) { // otherwise it's a heartbeat
                node.logdb.save(request.getEntries());
            } else {
                if (request.getCommittedIndex() > node.committedIndex) {
                    long size = request.getCommittedIndex() - request.getPrevLogIndex();
                    if (size < 100) {// 如果有许多的更新，就直接InstallSnapshot
                        for (long i = 1; i < size + 1; i++) {
                            LogEntry entry = (LogEntry) node.logdb.get(String.valueOf(request.getPrevLogIndex() + i));
                            if (entry != null) {
                                StateMachine.writeLogToDB(node, entry);
                            }
                        }
                        node.committedIndex = request.getCommittedIndex();
                    } else {
                        // install snapshot
                        return new ErrorResponse();
                    }
                }
            }
            return new AppendEntriesResponse(node.currentTerm, true, node.logdb.lastLogIndex);
        } finally {
            node.writeLock.unlock();
        }
    }
}
