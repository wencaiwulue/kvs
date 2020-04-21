package rpc;


import raft.Node;
import raft.NodeAddress;
import rpc.model.requestresponse.AddPeerRequest;
import rpc.model.requestresponse.Response;
import util.ThreadUtil;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * @author naison
 * @since 3/12/2020 11:09
 */
public class App {

    private final NIOServer nioServer;
    private final Node node;

    public App(InetSocketAddress address) {
        this(new NodeAddress(true, address));
    }

    public App(NodeAddress address) {
        this(address, new HashSet<>(Collections.singletonList(address)));
    }

    public App(NodeAddress address, Set<NodeAddress> allNodeAddresses) {
        this.node = new Node(address, allNodeAddresses);
        this.nioServer = new NIOServer(address.getSocketAddress(), this.node);
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
        if (port != 8000) {
            Response response = Client.doRequest(new NodeAddress(true, p8000), new AddPeerRequest(new NodeAddress(true, follower)));
            System.out.println(response);
        }
    }

}
