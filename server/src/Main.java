import main.db.NioServer;

public class Main {
    public static void main(String[] args) throws Throwable {
        new NioServer().start();
    }
}
