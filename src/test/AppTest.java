import org.junit.jupiter.api.Test;
import raft.NodeAddress;
import rpc.App;
import rpc.Client;
import rpc.model.requestresponse.AddPeerRequest;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashSet;

/**
 * @author naison
 * @since 4/12/2020 16:10
 */
class AppTest {

    private static final InetSocketAddress p8000 = new InetSocketAddress("localhost", 8000);
    private static final InetSocketAddress p8001 = new InetSocketAddress("localhost", 8001);
    private static final InetSocketAddress p8002 = new InetSocketAddress("localhost", 8002);
    private static final InetSocketAddress p8003 = new InetSocketAddress("localhost", 8003);
    private static final InetSocketAddress p8004 = new InetSocketAddress("localhost", 8004);
    private static final NodeAddress p0 = new NodeAddress(true, p8000);

    @Test
    void start() {
        new App(p8000).start();
        new App(p8001).start();
        new App(p8002).start();
        new App(p8003).start();
        Client.doRequest(p0, new AddPeerRequest(new NodeAddress(true, p8001)));
        Client.doRequest(p0, new AddPeerRequest(new NodeAddress(true, p8002)));
        Client.doRequest(p0, new AddPeerRequest(new NodeAddress(true, p8003)));
    }

    @Test
    void main() {
    }
}