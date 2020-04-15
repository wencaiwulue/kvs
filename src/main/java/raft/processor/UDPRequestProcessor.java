package raft.processor;

import raft.Node;
import rpc.model.requestresponse.Request;
import rpc.model.requestresponse.Response;

import java.nio.channels.SocketChannel;

/**
 * @author naison
 * @since 4/14/2020 09:26
 */
public class UDPRequestProcessor implements Processor {
    @Override
    public boolean supports(Request req) {
        return false;
    }

    @Override
    public Response process(Request req, Node node) {
        return null;
    }
}
