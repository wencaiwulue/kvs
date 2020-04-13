package raft.processor;

import raft.Node;

import java.nio.channels.SocketChannel;

/**
 * @author naison
 * @since 4/12/2020 17:08
 */
public interface Processor {

    boolean supports(Object req);

    // 这里要不要改成 Response process(Object req, Node node)??
    void process(Object req, Node node, SocketChannel channel);

}
