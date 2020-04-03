package rpc;


import raft.Node;
import thread.ThreadUtil;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author naison
 * @since 3/12/2020 11:09
 */
public class Server {

    private Poller poller;
    private Node node;
    private SocketPoller socketPoller;

    public Server(InetSocketAddress address, List<InetSocketAddress> peerAddress) throws Exception {
        this.poller = new Poller(address);
        this.node = new Node(address, peerAddress, poller.getSelector());
        poller.setNode(node);

        this.socketPoller = new SocketPoller(address, peerAddress);
        this.node.setSocketPoller(this.socketPoller);
        this.socketPoller.setNode(this.node);
    }

    public void start() {
        ThreadUtil.getPool().execute(poller);
        ThreadUtil.getPool().execute(node);
        ThreadUtil.getPool().execute(socketPoller);
    }

    public static void main(String[] args) throws Exception {
        String[] split = args[0].split(",");
        List<InetSocketAddress> peerAddr = Arrays.stream(split).skip(1).map(e -> new InetSocketAddress("localhost", Integer.parseInt(e))).collect(Collectors.toList());

        new Server(new InetSocketAddress("localhost", Integer.parseInt(split[0])), peerAddr).start();
    }

}
