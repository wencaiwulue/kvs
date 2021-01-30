import db.config.Config;
import db.core.DB;
import db.core.ExpireKey;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

public class DBTest {
    @Test
    public void test() {
        DB db = new DB(Config.DB_DIR);
        for (int i = 4; i > 0; i--) {
            db.set(String.valueOf(i), i, i, TimeUnit.MINUTES);
        }
        db.expireKey(String.valueOf(2), -1, TimeUnit.NANOSECONDS);
        db.expireKeys.remove(new ExpireKey(String.valueOf(3), 100, TimeUnit.MINUTES));
        System.out.println(db.expireKeys.size());

    }
}
