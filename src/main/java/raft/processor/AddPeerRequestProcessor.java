package raft.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.LogEntry;
import raft.Node;
import raft.NodeAddress;
import rpc.model.requestresponse.*;
import rpc.netty.RpcClient;
import util.ThreadUtil;

import java.util.List;

/**
 * @author naison
 * @since 4/12/2020 17:12
 */
public class AddPeerRequestProcessor implements Processor {

    private static final Logger LOGGER = LoggerFactory.getLogger(AddPeerRequestProcessor.class);

    @Override
    public boolean supports(Request req) {
        return req instanceof AddPeerRequest;
    }

    //  fake code:
    //
    //  if (node isn't leader) {
    //      if (leader is null) {
    //           means cluster is just start, every node don't know each other,
    //           return
    //      } else {
    //          if (sender == node.leader) {
    //             return
    //          } else {
    //             redirect to leader, let leader to notify echo nodes
    //        }
    //      }
    //  } else {
    //      notify echo nodes
    //      return
    //  }
    @Override
    public Response process(Request req, Node node) {
        AddPeerRequest request = (AddPeerRequest) req;

        if (!node.isLeader()) {
            if (node.getLeaderAddress() == null) {
                LOGGER.error("Not a cluster, just need to add to myself peer");
                node.getAllNodeAddresses().add(request.getPeer());
                return new AddPeerResponse();// exit 2
            } else {
                if (node.getLeaderAddress().equals(request.getSender())) {
                    node.getAllNodeAddresses().add(request.getPeer());
                    return new AddPeerResponse();// exit 3
                } else {
                    return RpcClient.doRequest(node.getLeaderAddress(), request);
                }
            }
        } else {
            // leader will notify all node to add new peer,
            // each node receive leader command, will send
            request.sender = node.getLeaderAddress();
            for (NodeAddress nodeAddress : node.allNodeAddressExcludeMe()) {
                RpcClient.doRequestAsync(nodeAddress, request, null);
            }
            node.getAllNodeAddresses().add(request.getPeer());
            // tell fresh bird to add all nodes
            RpcClient.doRequestAsync(request.getPeer(), new SynchronizeStateRequest(node.getLocalAddress(), node.getAllNodeAddresses()), null);
            // need to replicate log
            ThreadUtil.getThreadPool().submit(() -> {
                node.leaderReplicateLog();
                List<LogEntry> entries = node.getLogEntries().getRange(1, node.getLogEntries().getLastLogIndex() + 1);
                AppendEntriesRequest appendEntriesRequest = new AppendEntriesRequest(node.getCurrentTerm(), node.getLocalAddress(), 0, 0, entries, node.getCommittedIndex());
                RpcClient.doRequestAsync(request.getPeer(), appendEntriesRequest, re -> {
                    if (re instanceof AppendEntriesResponse) {
                        if (!((AppendEntriesResponse) re).isSuccess()) {
                            LOGGER.error("How to do ?");
                        }
                    } else if (re instanceof ErrorResponse) {
                        // out of data to much
                        InstallSnapshotRequest installSnapshotRequest = new InstallSnapshotRequest();
                        RpcClient.doRequestAsync(request.getPeer(), installSnapshotRequest, r -> {
                            if (r instanceof InstallSnapshotResponse) {
                                if (((InstallSnapshotResponse) r).getTerm() == node.getCurrentTerm()) {
                                    LOGGER.info("Replicate log successfully");
                                }
                            }
                        });
                    }
                });
            });
            return new AddPeerResponse();
        }

    }
}
