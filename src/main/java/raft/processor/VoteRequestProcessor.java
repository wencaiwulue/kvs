package raft.processor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import raft.Node;
import rpc.model.requestresponse.VoteRequest;
import rpc.model.requestresponse.VoteResponse;
import util.FSTUtil;

import java.io.IOException;
import java.net.InetSocketAddress;
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
    public boolean supports(Object obj) {
        return obj instanceof VoteRequest;
    }

    @Override
    public void process(Object obj, Node node, SocketChannel channel) {
        log.error("收到vote请求，{}", obj);
        try {
            VoteRequest request = (VoteRequest) obj;
            if (node.lastVoteFor == null && request.getTerm() > node.currTerm) {// 还没投过票
                channel.write(ByteBuffer.wrap(FSTUtil.getConf().asByteArray(new VoteResponse(request.getTerm(), true))));
                node.lastVoteFor = (InetSocketAddress) channel.getRemoteAddress();
            } else {
                channel.write(ByteBuffer.wrap(FSTUtil.getConf().asByteArray(new VoteResponse(request.getTerm(), false))));
            }
        } catch (ClosedChannelException e) {
            log.error("vote这里的channel又失效了");
        } catch (IOException e) {
            log.error("vote回复失败。{}", e.getMessage());
        }
    }
}
