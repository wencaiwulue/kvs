package main.db;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Set;

/**
 * @author fengcaiwen
 * @since 12/8/2019
 */
public class NioServer {

    public void start() throws Throwable {
        Selector selector = Selector.open();
        ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.bind(new InetSocketAddress("localhost", 8000));
        serverSocketChannel.configureBlocking(false);
        serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
        while (true) {
            int select = selector.select();
            if (select == 0) continue;

            Set<SelectionKey> selectionKeys = selector.selectedKeys();
            Iterator<SelectionKey> iterator = selectionKeys.iterator();
            while (iterator.hasNext()) {
                SelectionKey selectionKey = iterator.next();
                iterator.remove();
                SelectableChannel selectableChannel = selectionKey.channel();
                if (selectionKey.isAcceptable() || selectionKey.isConnectable()) {
                    acceptHandler(selectionKey, selector);
                } else if (selectionKey.isReadable()) {
                    readHandler((SocketChannel) selectableChannel, selector);
                }
            }
        }
    }

    private void acceptHandler(SelectionKey selectionKey, Selector selector) throws Exception {
        ServerSocketChannel serverSocketChannel = (ServerSocketChannel) selectionKey.channel();
        SocketChannel socketChannel = serverSocketChannel.accept();
        socketChannel.configureBlocking(false);
        socketChannel.register(selector, SelectionKey.OP_READ);
    }

    /*
     *
     * get set request
     * */
    private void readHandler(SocketChannel socketChannel, Selector selector) throws Exception {
        if (!socketChannel.isOpen()) return;

        ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
        StringBuilder request = new StringBuilder();
        int readLength;
        while ((readLength = socketChannel.read(byteBuffer)) > 0) {
            byteBuffer.flip();
            request.append(StandardCharsets.UTF_8.decode(byteBuffer));
        }
        /*
         * sun.nio.ch.IOStatus.EOF
         * sun.nio.ch.IOStatus.UNAVAILABLE
         * */
        if (readLength < 0) {
            socketChannel.close();
            return;
        }

        System.out.println(request);
        socketChannel.write(StandardCharsets.UTF_8.encode("ok, i got it , i am searching result"));
        socketChannel.register(selector, SelectionKey.OP_READ);
    }
}

