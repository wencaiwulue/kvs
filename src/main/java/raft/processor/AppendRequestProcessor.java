package raft.processor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import raft.LogEntry;
import raft.Node;
import rpc.Client;
import rpc.model.requestresponse.AppendRequest;
import rpc.model.requestresponse.AppendResponse;
import util.KryoUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

/**
 * @author naison
 * @since 4/13/2020 14:42
 */
public class AppendRequestProcessor implements Processor {
    private static final Logger log = LogManager.getLogger(AppendRequestProcessor.class);

    @Override
    public boolean supports(Object req) {
        return req instanceof AppendRequest;
    }

    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    @Override
    public void process(Object req, Node node, SocketChannel channel) {
        Object response;
        if (!node.isLeader()) {
            response = Client.doRequest(node.leaderAddr, req);
        } else {
            AppendRequest request = (AppendRequest) req;
            for (LogEntry log : request.getData()) {
                log.setIndex(++node.logdb.lastLogIndex);
                log.setTerm(node.currTerm);
            }
            node.getLogdb().save(request.getData());
            response = new AppendResponse();
        }
        try {
            channel.write(ByteBuffer.wrap(KryoUtil.asByteArray(response)));
        } catch (IOException e) {
            log.error(e);
        }
    }
}
