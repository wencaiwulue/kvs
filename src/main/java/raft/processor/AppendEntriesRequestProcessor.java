package raft.processor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import raft.Node;
import rpc.model.requestresponse.AppendEntriesRequest;
import rpc.model.requestresponse.AppendEntriesResponse;
import rpc.model.requestresponse.Request;
import rpc.model.requestresponse.Response;

/**
 * @author naison
 * @since 4/13/2020 14:42
 */
public class AppendEntriesRequestProcessor implements Processor {
    private static final Logger log = LogManager.getLogger(AppendEntriesRequestProcessor.class);

    @Override
    public boolean supports(Request req) {
        return req instanceof AppendEntriesRequest;
    }

    @Override
    public Response process(Request req, Node node) {
        node.writeLock.lock();
        try {
            AppendEntriesRequest request = (AppendEntriesRequest) req;
            node.logdb.save(request.getEntries());
            return new AppendEntriesResponse(node.currTerm, true, node.logdb.lastLogIndex);
        } finally {
            node.writeLock.unlock();
        }
    }
}
