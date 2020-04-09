import rpc.App;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class KvsApplication {

    public static void main(String[] args) {
        int base = 8000;
        int n = 2;
        List<InetSocketAddress> address = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            address.add(new InetSocketAddress("localhost", base + i));
        }

        for (InetSocketAddress s : address) {
            App app = new App(s, address.stream().filter(e -> !e.equals(s)).collect(Collectors.toList()));// 邻居不包含自己
            app.start();
        }
    }
}
