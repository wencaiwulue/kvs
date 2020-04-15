package rpc;


import raft.Node;
import rpc.model.requestresponse.AddPeerRequest;
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

    private NIOServer nioServer;
    private Node node;

    public App(InetSocketAddress address, Set<InetSocketAddress> peerAddress) throws IOException {
        this.node = new Node(address, peerAddress);
        this.nioServer = new NIOServer(address, this.node);
    }

    public void start() {
        ThreadUtil.getThreadPool().execute(nioServer);
        ThreadUtil.getThreadPool().execute(node);
    }

    public static void main(String[] args) throws IOException {
        App app = new App(new InetSocketAddress("localhost", 8000), new HashSet<>());
        app.start();
        App app1 = new App(new InetSocketAddress("localhost", 8001), new HashSet<>());
        app1.start();
        Client.doRequest(new InetSocketAddress("localhost", 8000), new AddPeerRequest(new InetSocketAddress("localhost", 8001)));
    }

}
