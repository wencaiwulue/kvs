package raft.processor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import raft.Node;
import raft.enums.State;
import rpc.model.requestresponse.HeartbeatRequest;
import rpc.model.requestresponse.HeartbeatResponse;
import rpc.model.requestresponse.Request;
import rpc.model.requestresponse.Response;

import java.net.InetSocketAddress;

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
        log.info("已经收到来自leader的心跳包, 其中包含了主节点的信息:{}, term:{}", request.getLeaderAddr(), request.getTerm());
        if (request.getTerm() > node.currTerm) {// 说明自己已经out了，需要更新主节点信息
            node.currTerm = request.getTerm();
            node.state = State.FOLLOWER;
            node.leaderAddr = request.getLeaderAddr();
            node.lastVoteFor = null;// last vote info should clean
        }
        return new HeartbeatResponse();
    }
}
