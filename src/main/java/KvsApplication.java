import raft.Node;
import raft.NodeAddress;
import rpc.model.requestresponse.Request;
import rpc.model.requestresponse.Response;
import rpc.netty.server.WebSocketServer;
import util.ThreadUtil;

import java.net.InetSocketAddress;
import java.util.function.Function;


public class KvsApplication {
    public static void main(String[] args) {
        InetSocketAddress local = new InetSocketAddress("127.0.0.1", Integer.parseInt(System.getenv("port")));
        Node node = Node.of(new NodeAddress(local));

        ThreadUtil.getThreadPool().execute(node::start);
        new WebSocketServer(local, node::handle).start();
    }
}
