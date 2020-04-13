package raft.processor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import raft.Node;
import raft.State;
import rpc.model.Response;
import rpc.model.requestresponse.VoteRequest;
import rpc.model.requestresponse.VoteResponse;
import util.FSTUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;

/**
 * @author naison
 * @since 4/12/2020 17:56
 */
public class VoteRequestProcessor implements Processor {

    private static final Logger log = LogManager.getLogger(VoteRequestProcessor.class);

    @Override
    public boolean supports(Object req) {
        return req instanceof VoteRequest;
    }

    @Override
    public void process(Object req, Node node, SocketChannel channel) {
        node.getWriteLock().lock();
        try {
            VoteRequest request = (VoteRequest) req;
            log.error("收到vote请求.from:{} to {}, vote info:{}", request.getPeer(), node.getAddress(), request);
            Response response;
            int i = Long.compare(request.getLastLogTerm(), node.getLogDB().getLastLogTerm());
            if (i == 0) i = Long.compare(request.getLastLogIndex(), node.getLogDB().getLastLogIndex());
            if (i >= 0 && node.lastVoteFor == null) {// 还没投过票
                node.lastVoteFor = request.getPeer();
                node.currTerm = request.getTerm();
                node.leaderAddr = null;
                node.state = State.FOLLOWER;
                response = new VoteResponse(request.getTerm(), true);
            } else {
                response = new VoteResponse(request.getTerm(), false);
            }
            try {
                channel.write(ByteBuffer.wrap(FSTUtil.getConf().asByteArray(response)));
            } catch (ClosedChannelException e) {
                log.error("vote这里的channel又失效了");
            } catch (IOException e) {
                log.error("vote回复失败。{}", e.getMessage());
            }
        } finally {
            node.getWriteLock().unlock();
        }
    }
}
