package raft.processor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import raft.Node;
import rpc.model.requestresponse.AddPeerRequest;
import rpc.model.requestresponse.AddPeerResponse;
import util.FSTUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;

/**
 * @author naison
 * @since 4/12/2020 17:12
 */
public class AddPeerRequestProcessor implements Processor {

    private static final Logger log = LogManager.getLogger(AddPeerRequestProcessor.class);

    @Override
    public boolean supports(Object obj) {
        return obj instanceof AddPeerRequest;
    }

    @Override
    public void process(Object obj, Node node, SocketChannel channel) {
        AddPeerRequest request = (AddPeerRequest) obj;
        node.getPeerAddress().add(request.getPeer());
        try {
            // todo 从un-commit中拿去日志，还要从主节点中拿取数据，也就是自己out事件里，世界的变化
            channel.write(ByteBuffer.wrap(FSTUtil.getConf().asByteArray(new AddPeerResponse())));// 先构成一次完整的rpc
        } catch (ClosedChannelException e) {
            log.error("这里的channel又失效了");
        } catch (IOException e) {
            log.error("回复失败。{}", e.getMessage());
        }
    }
}
