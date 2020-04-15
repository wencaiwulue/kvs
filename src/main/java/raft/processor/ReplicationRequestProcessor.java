package raft.processor;

import raft.Node;

import java.nio.channels.SocketChannel;

/**
 * @author naison
 * @since 4/13/2020 22:38
 */
public class ReplicationRequestProcessor implements Processor {
    @Override
    public boolean supports(Object req) {
        return false;
    }

    @Override
    public void process(Object req, Node node, SocketChannel channel) {

    }
}
