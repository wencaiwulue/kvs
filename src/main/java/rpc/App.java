package rpc;


import db.core.Config;
import raft.Node;
import raft.NodeAddress;
import util.ThreadUtil;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author naison
 * @since 3/12/2020 11:09
 */
public class App {

    private final RpcServer rpcServer;
    private final Node node;

    public App(InetSocketAddress address) {
        this(new NodeAddress(true, address));
    }

    public App(NodeAddress address) {
        this(address, new HashSet<>(Collections.singletonList(address)));
    }

    public App(NodeAddress address, Set<NodeAddress> allNodeAddresses) {
        this.node = new Node(address, allNodeAddresses);
        this.rpcServer = new RpcServer(address.getSocketAddress(), this.node);
    }

    public void start() {
        ThreadUtil.getThreadPool().submit(rpcServer);
        ThreadUtil.getThreadPool().submit(node);
    }

    public static void main(String[] args) {
        InetSocketAddress follower = new InetSocketAddress("localhost", Config.PORT);
        new App(follower).start();
    }

}
