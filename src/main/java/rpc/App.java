package rpc;


import raft.Node;
import rpc.model.requestresponse.AddPeerRequest;
import util.ThreadUtil;

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

    public App(InetSocketAddress address) {
        this(address, new HashSet<>());
    }

    public App(InetSocketAddress address, Set<InetSocketAddress> peerAddress) {
        this.node = new Node(address, peerAddress);
        this.nioServer = new NIOServer(address, this.node);
    }

    public void start() {
        ThreadUtil.getThreadPool().execute(nioServer);
        ThreadUtil.getThreadPool().execute(node);
    }

    public static void main(String[] args) {
        InetSocketAddress p8000 = new InetSocketAddress("localhost", 8000);
        InetSocketAddress p8001 = new InetSocketAddress("localhost", 8001);
        new App(p8000).start();
        new App(p8001).start();
        Client.doRequest(p8000, new AddPeerRequest(p8001));
    }

}
