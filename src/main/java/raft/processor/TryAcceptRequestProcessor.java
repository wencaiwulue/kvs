package raft.processor;

import raft.Node;

import java.nio.channels.SocketChannel;

/**
 * @author naison
 * @since 4/12/2020 18:06
 */
public class TryAcceptRequestProcessor implements Processor {
    @Override
    public boolean supports(Object obj) {
        return false;
    }

    @Override
    public void process(Object obj, Node node, SocketChannel channel) {

    }
}
