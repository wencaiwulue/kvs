package main.request;

import java.net.InetSocketAddress;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

/**
 * @author fengcaiwen
 * @since 12/8/2019
 */
public class NioClient {

    public static void main(String[] args) throws Throwable {
        start();
    }

    private static void start() throws Throwable {
        Selector selector = Selector.open();
        SocketChannel channel = SocketChannel.open(new InetSocketAddress("localhost", 8000));
        channel.configureBlocking(false);
        NioClientReadHandler readHandler = new NioClientReadHandler(selector);
        readHandler.writeHandler(channel);
        readHandler.run();
    }
}
