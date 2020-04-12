package rpc;


import raft.Node;
import util.ThreadUtil;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author naison
 * @since 3/12/2020 11:09
 */
public class App {

    private NioServer nioServer;
    private Node node;

    public App(InetSocketAddress address, List<InetSocketAddress> peerAddress) throws IOException {
        this.node = new Node(address, peerAddress);
        this.nioServer = new NioServer(address, this.node);
    }

    public void start() {
        ThreadUtil.getThreadPool().execute(nioServer);
        ThreadUtil.getThreadPool().execute(node);
    }

    public static void main(String[] args) throws IOException {
        new App(new InetSocketAddress("localhost", 8000), new ArrayList<>()).start();
    }

}
