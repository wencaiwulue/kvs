import rpc.App;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

public class KvsApplication {

    public static void main(String[] args) throws IOException {
        int base = 8000;
        int n = 2;
        List<InetSocketAddress> address = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            address.add(new InetSocketAddress("localhost", base + i));
        }

        for (InetSocketAddress s : address) {
            App app = new App(s, new ArrayList<>());// 邻居不包含自己
            app.start();
        }
    }
}
