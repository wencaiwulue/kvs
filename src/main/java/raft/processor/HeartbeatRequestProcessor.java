package raft.processor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import raft.Node;
import raft.enums.Role;
import rpc.model.requestresponse.HeartbeatRequest;
import rpc.model.requestresponse.HeartbeatResponse;
import rpc.model.requestresponse.Request;
import rpc.model.requestresponse.Response;

/**
 * @author naison
 * @since 4/12/2020 17:52
 */
public class HeartbeatRequestProcessor implements Processor {

    private static final Logger log = LogManager.getLogger(HeartbeatRequestProcessor.class);

    @Override
    public boolean supports(Request req) {
        return req instanceof HeartbeatRequest;
    }

    @Override
    public Response process(Request req, Node node) {
        HeartbeatRequest request = (HeartbeatRequest) req;
        node.lastHeartBeat = System.nanoTime();
        log.error("{}收到来自leader的心跳包, leader info:{}", node.address.socketAddress.getPort(), request);
        if (request.getTerm() > node.currTerm) {// 说明自己已经out了，需要更新主节点信息
            node.currTerm = request.getTerm();
            node.role = Role.FOLLOWER;
            node.leaderAddress = request.getLeaderAddr();
            node.lastVoteFor = null;// last vote info should clean
        }
        return new HeartbeatResponse();
    }
}
