import org.junit.jupiter.api.Test;
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

    private static InetSocketAddress p8000 = new InetSocketAddress("localhost", 8000);
    private static InetSocketAddress p8001 = new InetSocketAddress("localhost", 8001);
    private static InetSocketAddress p8002 = new InetSocketAddress("localhost", 8002);
    private static InetSocketAddress p8003 = new InetSocketAddress("localhost", 8003);
    private static InetSocketAddress p8004 = new InetSocketAddress("localhost", 8004);

    @Test
    void start() {
        new App(p8000).start();
        new App(p8001).start();
        new App(p8002).start();
        new App(p8003).start();
        Client.doRequest(p8000, new AddPeerRequest(p8001));
        Client.doRequest(p8000, new AddPeerRequest(p8002));
        Client.doRequest(p8000, new AddPeerRequest(p8003));
    }

    @Test
    void main() {
    }
}