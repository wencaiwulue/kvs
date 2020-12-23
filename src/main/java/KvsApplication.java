import raft.NodeAddress;
import rpc.App;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

public class KvsApplication {
//
//    public static void main(String[] args) {
//        int base = 8000;
//        int n = 3;
//        List<InetSocketAddress> address = new ArrayList<>(n);
//        for (int i = 0; i < n; i++) {
//            address.add(new InetSocketAddress("localhost", base + i));
//        }
//
//        for (InetSocketAddress s : address) {
//            App app = new App(s);// 邻居不包含自己
//            app.start();
//        }
//    }


    public static void main(String[] args) {
        int port = Integer.parseInt(args[0]);
//        NodeAddress localhost0 = new NodeAddress(true, new InetSocketAddress("localhost", 8000));
//        NodeAddress localhost1 = new NodeAddress(true, new InetSocketAddress("localhost", 8001));
//        NodeAddress localhost2 = new NodeAddress(true, new InetSocketAddress("localhost", 8002));
//        HashSet<NodeAddress> nodeAddresses = new HashSet<>(Arrays.asList(localhost0, localhost1, localhost2));
        App app = new App(new NodeAddress(true, new InetSocketAddress("localhost", port))/*, nodeAddresses*/);
        app.start();
    }
}
