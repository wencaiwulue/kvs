import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import raft.NodeAddress;
import raft.enums.CURDOperation;
import rpc.model.requestresponse.AddPeerRequest;
import rpc.model.requestresponse.CURDKVRequest;
import rpc.model.requestresponse.CURDResponse;
import rpc.model.requestresponse.RemovePeerRequest;
import rpc.model.requestresponse.Response;
import util.NettyClientTest;

import java.net.InetSocketAddress;

/**
 * @author naison
 * @since 4/12/2020 16:10
 */
class AppTest {
    private static final InetSocketAddress p8001 = new InetSocketAddress("localhost", 8001);
    private static final InetSocketAddress p8002 = new InetSocketAddress("localhost", 8002);
    private static final InetSocketAddress p8003 = new InetSocketAddress("localhost", 8003);
    private static final InetSocketAddress p8004 = new InetSocketAddress("localhost", 8004);
    private static final InetSocketAddress p8005 = new InetSocketAddress("localhost", 8005);

    @Test
    void addPeer1And2And3() throws Exception {
        NettyClientTest.sendAsync(p8001, new AddPeerRequest(new NodeAddress(p8002)));
        NettyClientTest.sendAsync(p8001, new AddPeerRequest(new NodeAddress(p8003)));
//        NettyClientTest.sendAsync(p8002, new AddPeerRequest(new NodeAddress(p8001)));
    }

    @Test
    void addPeer1And2() throws Exception {
        NettyClientTest.sendAsync(p8001, new AddPeerRequest(new NodeAddress(p8002)));
        NettyClientTest.sendAsync(p8002, new AddPeerRequest(new NodeAddress(p8001)));
    }

    @Test
    void addPeer3() throws Exception {
        NettyClientTest.sendAsync(p8002, new AddPeerRequest(new NodeAddress(p8003)));
    }

    @Test
    void addPeer4() throws Exception {
        NettyClientTest.sendAsync(p8002, new AddPeerRequest(new NodeAddress(p8004)));
    }

    @Test
    void addPeer5() throws Exception {
        NettyClientTest.sendAsync(p8001, new AddPeerRequest(new NodeAddress(p8005)));
    }

    @Test
    void removePeer1() throws Exception {
        NettyClientTest.sendAsync(p8001, new RemovePeerRequest(new NodeAddress(p8001)));
    }

    @Test
    void removePeer2() throws Exception {
        NettyClientTest.sendAsync(p8004, new RemovePeerRequest(new NodeAddress(p8002)));
    }

    @Test
    void addData() throws Exception {
        Response res = NettyClientTest.sendSync(p8001, new CURDKVRequest(CURDOperation.set, new String[]{"a"}, new Object[]{10}));
        System.out.println(((CURDResponse) res).success);
    }

    @Test
    void getData() throws Exception {
        Response res = NettyClientTest.sendSync(p8003, new CURDKVRequest(CURDOperation.get, new String[]{"a"}, null));
        System.out.println(((CURDResponse) res).value[0]);
    }

    @Test
    void testAddAndGetData() throws Exception {
        int value = 10;
        NettyClientTest.sendSync(p8001, new CURDKVRequest(CURDOperation.set, new String[]{"a"}, new Object[]{value}));
        Response res = NettyClientTest.sendSync(p8003, new CURDKVRequest(CURDOperation.get, new String[]{"a"}, null));
        Assertions.assertEquals(value, ((CURDResponse) res).value[0]);
    }

}
