package rpc;


import raft.Node;
import rpc.model.requestresponse.AddPeerRequest;
import rpc.model.requestresponse.Response;
import util.ThreadUtil;

import java.net.InetSocketAddress;
import java.util.Arrays;
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
        InetSocketAddress p8002 = new InetSocketAddress("localhost", 8002);
        int port = Integer.parseInt(args[0]);
        InetSocketAddress follower = new InetSocketAddress("localhost", port);
        new App(follower).start();
        ThreadUtil.sleep(5000);
        if (port != 8000) {
            Response response = Client.doRequest(p8000, new AddPeerRequest(follower));
            System.out.println(response);
        }
    }

}
