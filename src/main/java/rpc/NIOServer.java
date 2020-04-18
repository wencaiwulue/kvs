package rpc;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import raft.Node;
import rpc.model.requestresponse.Request;
import util.FSTUtil;
import util.ThreadUtil;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.Iterator;

/**
 * @author naison
 * @since 3/25/2020 19:32
 */
public class NIOServer implements Runnable {
    private static final Logger log = LogManager.getLogger(NIOServer.class);

    private Selector selector;
    private Node node;

    private volatile boolean close = false;

    public NIOServer(InetSocketAddress addr, Node node) {
        try {
            this.selector = Selector.open();
            this.bind(addr);
            this.node = node;
        } catch (IOException e) {
            log.error("during start occurs error, shutting down...");
            this.destroy();
            Runtime.getRuntime().exit(-1);
        }
    }

    private void bind(InetSocketAddress addr) throws IOException {
        ServerSocketChannel serverSocket = ServerSocketChannel.open();
        serverSocket.bind(addr);
        serverSocket.configureBlocking(false);
        serverSocket.register(selector, SelectionKey.OP_ACCEPT);
        log.error("服务已启动，已经绑定{}", addr);
    }

    private void destroy() {
        close = true;
        selector.wakeup();
    }

    @Override
    public void run() {
        // Loop until destroy() is called
        while (true) {
            int keyCount = 0;
            try {
                if (!close) {
                    keyCount = selector.selectNow();
                }
                if (close) {
                    try {
                        selector.close();
                    } catch (IOException ioe) {
                        log.error("关闭selector出错楼", ioe);
                    }
                    break;
                }
            } catch (Throwable x) {
                log.error("", x);
                continue;
            }

            Iterator<SelectionKey> iterator = keyCount > 0 ? selector.selectedKeys().iterator() : null;
            while (iterator != null && iterator.hasNext()) {
                SelectionKey sk = iterator.next();
                iterator.remove();
                processKey(sk);
            }//while
        }//while
    }

    private void processKey(SelectionKey selectionKey) {
        try {
            if (close) {
                cancelledKey(selectionKey);
            } else if (selectionKey.isValid()) {
                if (selectionKey.isAcceptable()) {
                    ServerSocketChannel serverSocketChannel = (ServerSocketChannel) selectionKey.channel();
                    SocketChannel channel = serverSocketChannel.accept();
                    channel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
                    channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
                    channel.configureBlocking(false);
                    channel.register(selector, SelectionKey.OP_READ);
                    log.error("已经创建链接:{}", channel.getRemoteAddress());
                } else if (selectionKey.isReadable()) {
                    boolean closeSocket = false;
                    if (!processRead(selectionKey)) {
                        closeSocket = true;
                    }
                    if (closeSocket) {
                        cancelledKey(selectionKey);
                    }
                }
            } else {
                cancelledKey(selectionKey);
            }
        } catch (CancelledKeyException e) {
            cancelledKey(selectionKey);
        } catch (Throwable t) {
            log.error("这次戳错啦", t);
        }
    }

    private void cancelledKey(SelectionKey key) {
        if (key == null) {
            return;
        }

        SocketChannel socketChannel;
        socketChannel = (SocketChannel) key.attach(null);
        if (socketChannel != null) {
            if (socketChannel.isConnected() && socketChannel.isOpen()) {
                try {
                    socketChannel.close();
                } catch (Exception e) {
                    log.error("关闭socketChannel出错啦！！！，{}", e.getMessage());
                }
            }
        }
        if (key.channel().isOpen()) {
            try {
                key.channel().close();
            } catch (Exception e) {
                log.error("关闭channel出错啦！！！，{}", e.getMessage());
            }
        }
    }

    // todo nio
    private boolean processRead(SelectionKey key) {
        try {
            ThreadUtil.getThreadPool().execute(new Handler(key, node));
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    public static class Handler implements Runnable {

        private SelectionKey key;
        private Node node;

        private Handler(SelectionKey key, Node node) {
            this.key = key;
            this.node = node;
        }

        @Override
        public void run() {
            SocketChannel channel = (SocketChannel) key.channel();
            ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
            if (channel != null /*&& channel.isOpen() && channel.isConnected()*/) {
                try {
                    int read = channel.read(byteBuffer);
                    if (read <= 0) return;// 也就是客户端主动断开链接，因为在客户端断开的时候，也会发送一个读事件

                    Request o = (Request) FSTUtil.getConf().asObject(byteBuffer.array());
                    this.node.handle(o, channel);// handle the request
                } catch (IOException e) {
                    log.error("出错了，关闭channel", e);
                    try {
                        channel.close();
                    } catch (Exception ex) {
                        log.error(ex);
                    }
                    this.key.cancel();
                }
            } else {
                log.error("channel disconnect, should retry or not ?");
            }
        }
    }
}
