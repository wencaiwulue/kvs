import org.junit.jupiter.api.Test;
import raft.NodeAddress;
import raft.enums.CURDOperation;
import rpc.Client;
import rpc.model.requestresponse.AddPeerRequest;
import rpc.model.requestresponse.CURDKVRequest;
import rpc.model.requestresponse.RemovePeerRequest;

import java.net.InetSocketAddress;

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
    private static final NodeAddress p1 = new NodeAddress(true, p8001);
    private static final NodeAddress p2 = new NodeAddress(true, p8002);

    @Test
    void addPeer() {
        Client.doRequest(p0, new AddPeerRequest(new NodeAddress(true, p8001)));
        Client.doRequest(p1, new AddPeerRequest(new NodeAddress(true, p8002)));
    }

    @Test
    void removePeer() {
        Client.doRequest(p2, new RemovePeerRequest(new NodeAddress(true, p8000)));
    }

    @Test
    void curd() {
        Client.doRequest(p0, new CURDKVRequest(CURDOperation.set, "a", 1));
        Client.doRequest(p1, new CURDKVRequest(CURDOperation.set, "b", 2));


    }

}