package rpc;


import raft.Node;
import util.ThreadUtil;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author naison
 * @since 3/12/2020 11:09
 */
public class App {

    private NioServer nioServer;
    private Node node;

    public App(InetSocketAddress address, List<InetSocketAddress> peerAddress) {
        this.node = new Node(address, peerAddress);
        this.nioServer = new NioServer(address, this.node);
    }

    public void start() {
        ThreadUtil.getPool().execute(nioServer);
        ThreadUtil.getPool().execute(node);
    }

    public static void main(String[] args) {
        String[] split = args[0].split(",");
        List<InetSocketAddress> peerAddr = Arrays.stream(split).skip(1).map(e -> new InetSocketAddress("localhost", Integer.parseInt(e))).collect(Collectors.toList());

        new App(new InetSocketAddress("localhost", Integer.parseInt(split[0])), peerAddr).start();
    }

}
