import raft.Node;
import raft.NodeAddress;
import rpc.netty.server.WebSocketServer;


public class KvsApplication {
    public static void main(String[] args) {
        Node node = Node.of(new NodeAddress(WebSocketServer.SELF_ADDRESS));
        WebSocketServer.main(node);
    }
}
