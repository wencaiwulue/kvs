import com.google.common.base.Strings;
import lombok.extern.slf4j.Slf4j;
import raft.Node;
import raft.NodeAddress;
import rpc.netty.server.WebSocketServer;
import util.ThreadUtil;

import java.net.InetSocketAddress;

@Slf4j
public class KvsApplication {

    public static void main(String[] args) {
        String portStr = System.getenv("port");
        if (Strings.isNullOrEmpty(portStr)) {
            log.error("Not set port, needs to set it! exiting");
            System.exit(-1);
        }
        int port = 0;
        try {
            port = Integer.parseInt(portStr);
        } catch (NumberFormatException exception) {
            log.error("Can not parse string: {} to int, exiting", portStr);
            System.exit(-2);
        }


        InetSocketAddress local = new InetSocketAddress("127.0.0.1", port);
        Node node = Node.of(new NodeAddress(local));

        ThreadUtil.getThreadPool().execute(node::start);
        new WebSocketServer(local, node::handle).start();
    }
}
