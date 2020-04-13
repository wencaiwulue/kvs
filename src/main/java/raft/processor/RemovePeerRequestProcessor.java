package raft.processor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import raft.Node;
import rpc.model.requestresponse.*;
import util.FSTUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;

/**
 * @author naison
 * @since 4/12/2020 17:12
 */
public class RemovePeerRequestProcessor implements Processor {

    private static final Logger log = LogManager.getLogger(RemovePeerRequestProcessor.class);

    @Override
    public boolean supports(Object req) {
        return req instanceof RemovePeerRequest;
    }

    @Override
    public void process(Object req, Node node, SocketChannel channel) {
        RemovePeerRequest request = (RemovePeerRequest) req;
        boolean remove = node.getPeerAddress().remove(request.getPeer());
        try {
            if (remove) {
                channel.write(ByteBuffer.wrap(FSTUtil.getConf().asByteArray(new RemovePeerResponse())));
            } else {
                channel.write(ByteBuffer.wrap(FSTUtil.getConf().asByteArray(new ErrorResponse(400, "remove peer failed, because don`t exists node: " + request.getPeer()))));
            }
        } catch (ClosedChannelException e) {
            log.error("这里的channel又失效了");
        } catch (IOException e) {
            log.error("remove peer回复失败。", e);
        }
    }
}
