package raft.processor;

import raft.Node;
import rpc.model.requestresponse.SetRequest;

import java.nio.channels.SocketChannel;

/**
 * @author naison
 * @since 4/13/2020 14:42
 */
public class SetRequestProcessor implements Processor {
    @Override
    public boolean supports(Object req) {
        return req instanceof SetRequest;
    }

    @Override
    public void process(Object req, Node node, SocketChannel channel) {

    }
}
