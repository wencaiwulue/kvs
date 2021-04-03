import db.config.Config;
import db.core.DB;
import db.core.pojo.ExpireKey;
import org.junit.jupiter.api.Test;
import rpc.netty.server.WebSocketServer;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

public class DBTest {
    @Test
    public void test() {
        int port = Integer.parseInt(System.getenv("port"));
        Path path = Path.of(System.getProperty("user.dir"), "data", String.valueOf(port), "db");
        DB db = new DB(path);
        for (int i = 4; i > 0; i--) {
            db.set(String.valueOf(i), i, i, TimeUnit.MINUTES);
        }
        db.expireKey(String.valueOf(2), -1, TimeUnit.NANOSECONDS);
        db.getExpireKeys().remove(new ExpireKey(100, TimeUnit.MINUTES, String.valueOf(3)));
        System.out.println(db.getExpireKeys().size());
    }
}
