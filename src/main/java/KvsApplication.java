import rpc.App;

import java.net.InetSocketAddress;
import java.util.ArrayList;
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
        App app = new App(new InetSocketAddress("localhost", port));
        app.start();
    }
}
