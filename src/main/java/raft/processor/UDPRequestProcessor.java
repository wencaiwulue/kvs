package raft.processor;

import raft.Node;

import java.nio.channels.SocketChannel;

/**
 * @author naison
 * @since 4/14/2020 09:26
 */
public class UDPRequestProcessor implements Processor {
    @Override
    public boolean supports(Object req) {
        return false;
    }

    @Override
    public void process(Object req, Node node, SocketChannel channel) {

    }
}
