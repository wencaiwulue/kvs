package raft.processor;

import raft.Node;

import java.nio.channels.SocketChannel;

/**
 * @author naison
 * @since 4/12/2020 17:08
 */
public interface Processor {

    boolean supports(Object obj);

    void process(Object obj, Node node, SocketChannel channel);

}
