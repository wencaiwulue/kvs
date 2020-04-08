package main.request;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Set;

/**
 * @author fengcaiwen
 * @since 12/8/2019
 */
public class NioClientReadHandler implements Runnable {

    private Selector selector;

    NioClientReadHandler(Selector selector) {
        this.selector = selector;
    }

    @Override
    public void run() {
        try {
            while (true) {
                int select = selector.select();
                if (select == 0) continue;

                Set<SelectionKey> selectionKeys = selector.selectedKeys();
                Iterator<SelectionKey> iterator = selectionKeys.iterator();
                while (iterator.hasNext()) {
                    SelectionKey selectionKey = iterator.next();
                    iterator.remove();
                    if (selectionKey.isReadable()) {
                        readHandler((SocketChannel) selectionKey.channel());
                        return;
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void readHandler(SocketChannel channel) throws IOException {
        ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
        StringBuilder str = new StringBuilder();
        while (channel.read(byteBuffer) > 0) {
            byteBuffer.flip();
            str.append(StandardCharsets.UTF_8.decode(byteBuffer));
        }
        channel.close();
        System.out.println(str);
    }

    void writeHandler(SocketChannel channel) throws IOException {
        channel.write(ByteBuffer.wrap("i am client".getBytes()));
        channel.register(selector, SelectionKey.OP_READ);
    }
}
