package raft.processor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import raft.LogEntry;
import raft.Node;
import rpc.Client;
import rpc.model.requestresponse.AppendEntriesRequest;
import rpc.model.requestresponse.AppendEntriesResponse;
import util.KryoUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

/**
 * @author naison
 * @since 4/13/2020 14:42
 */
public class AppendEntriesRequestProcessor implements Processor {
    private static final Logger log = LogManager.getLogger(AppendEntriesRequestProcessor.class);

    @Override
    public boolean supports(Object req) {
        return req instanceof AppendEntriesRequest;
    }

    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    @Override
    public void process(Object req, Node node, SocketChannel channel) {
        Object response;
        if (!node.isLeader()) {
            response = Client.doRequest(node.leaderAddr, req);
        } else {
            AppendEntriesRequest request = (AppendEntriesRequest) req;
            for (LogEntry log : request.getData()) {
                log.setIndex(++node.logdb.lastLogIndex);
                log.setTerm(node.currTerm);
            }
            node.getLogdb().save(request.getData());
            response = new AppendEntriesResponse();
        }
        try {
            channel.write(ByteBuffer.wrap(KryoUtil.asByteArray(response)));
        } catch (IOException e) {
            log.error(e);
        }
    }
}
