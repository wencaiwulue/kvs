package rpc;


import raft.Node;
import util.ThreadUtil;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;

/**
 * @author naison
 * @since 3/12/2020 11:09
 */
public class App {

    private NioServer nioServer;
    private Node node;

    public App(InetSocketAddress address, Set<InetSocketAddress> peerAddress) throws IOException {
        this.node = new Node(address, peerAddress);
        this.nioServer = new NioServer(address, this.node);
    }

    public void start() {
        ThreadUtil.getThreadPool().execute(nioServer);
        ThreadUtil.getThreadPool().execute(node);
    }

    public static void main(String[] args) throws IOException {
        new App(new InetSocketAddress("localhost", 8000), new HashSet<>()).start();
    }

}
