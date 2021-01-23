package raft.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.Node;
import raft.NodeAddress;
import rpc.model.requestresponse.*;
import rpc.netty.pub.RpcClient;

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

        node.allNodeAddresses.add(request.getPeer());

//        if (request.getPeer().equals(request.getSender())) {
//            return new AddPeerResponse();// 非主节点，终结 exit 1
//        }

        if (!node.isLeader()) {
            if (node.leaderAddress == null) {
                return new AddPeerResponse();// exit 2
            } else {
                if (node.leaderAddress.equals(request.getSender())) {
                  return new AddPeerResponse();// exit 3
                } else {
                  return RpcClient.doRequest(node.leaderAddress, request);
                }
            }
        } else {
            // leader will notify all node to add new peer,
            // each node receive leader command, will send
            request.sender = node.leaderAddress;
            for (NodeAddress nodeAddress : node.allNodeAddressExcludeMe()) {
                RpcClient.doRequest(nodeAddress, request);
            }
            // means this node is just power up, need to synchronize data
            PowerResponse response = (PowerResponse) RpcClient.doRequest(request.getPeer(), new PowerRequest(false, false));
            if (response != null && response.isSuccess()) {
                return new AddPeerResponse();
            } else {
                return new ErrorResponse("add peer failed, peer info: " + request.getPeer());
            }
        }

    }
}
