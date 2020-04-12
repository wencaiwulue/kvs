package raft.processor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import raft.Node;
import raft.State;
import rpc.model.requestresponse.HeartbeatRequest;
import rpc.model.requestresponse.HeartbeatResponse;
import util.KryoUtil;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;

/**
 * @author naison
 * @since 4/12/2020 17:52
 */
public class HeartbeatRequestProcessor implements Processor {

    private static final Logger log = LogManager.getLogger(HeartbeatRequestProcessor.class);

    @Override
    public boolean supports(Object obj) {
        return obj instanceof HeartbeatRequest;
    }

    @Override
    public void process(Object obj, Node node, SocketChannel channel) {
        HeartbeatRequest request = (HeartbeatRequest) obj;
        node.lastHeartBeat = System.nanoTime();
        InetSocketAddress leaderAddr = request.getLeaderAddr();
        int term = request.getTerm();
        log.error("已经收到来自leader的心跳包, 其中包含了主节点的信息，{}, term:{}", leaderAddr, term);
        if (term > node.currTerm) {// 说明自己已经out了，需要更新主节点信息
            node.leaderAddr = leaderAddr;
            node.state = State.FOLLOWER;
            node.currTerm = term;
        }
        try {
            channel.write(ByteBuffer.wrap(KryoUtil.asByteArray(new HeartbeatResponse())));// 先构成一次完整的rpc
        } catch (ClosedChannelException e) {
            log.error("心跳包这里的channel又失效了");
        } catch (IOException e) {
            log.error("心跳包回复失败。{}", e.getMessage());
        }
    }
}
