package rpc;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.util.Assert;
import raft.Node;
import rpc.model.Request;
import rpc.model.Response;
import thread.FSTUtil;
import thread.ThreadUtil;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Poller class.
 *
 * @author naison
 * @since 3/25/2020 19:32
 */
public class Poller implements Runnable {
    private static final Logger log = LogManager.getLogger(Poller.class);

    private Selector selector;
    private Node node;

    private volatile boolean close = false;

    public Poller(InetSocketAddress addr) throws Exception {
        this.selector = Selector.open();
        this.bind(addr);
    }

    public Selector getSelector() {
        return selector;
    }

    public void setNode(Node node) {
        this.node = node;
    }

    private void bind(InetSocketAddress addr) throws Exception {
        ServerSocketChannel serverSocket = ServerSocketChannel.open();
        serverSocket.bind(addr);
        serverSocket.configureBlocking(false);
        serverSocket.register(selector, SelectionKey.OP_ACCEPT);
        log.error("服务已启动，已经绑定{}", addr);
    }

    protected void destroy() {
        close = true;
        selector.wakeup();
    }

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
                    log.error("已经创建链接{}", channel.getRemoteAddress());
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
        Future<Boolean> submit = ThreadUtil.getPool().submit(new Handle(key, selector, node));
        try {
            return submit.get(100, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            submit.cancel(false);
            return false;
        }
    }

    public static class Handle implements Callable<Boolean> {

        private SelectionKey key;
        private Selector selector;
        private Node node;

        private Handle(SelectionKey key, Selector selector, Node node) {
            this.key = key;
            this.selector = selector;
            this.node = node;
        }

        @Override
        public Boolean call() {
            SocketChannel channel = (SocketChannel) key.channel();
            ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
            if (channel != null && channel.isOpen() && channel.isConnected()) {
                try {
                    int read = channel.read(byteBuffer);
                    if (read <= 0) {
                        return false;// 也就是客户端主动断开链接，因为在客户端断开的时候，也会发送一个读事件
                    }

                    Object o = FSTUtil.getConf().asObject(byteBuffer.array());
                    Request request = null;
                    if (o instanceof Response) {
                        log.error("为什么这里是response呢，奇怪.{}", o);// 其实这里可能是不要返回包的request，但是现在的项目里是不允许这样的
                        return true;
                    } else if (o instanceof Request) {
                        request = (Request) o;
                    }
                    Assert.requireNonEmpty(node, "this is not impossible!!!");
                    node.service(request, channel, selector);// handle the request
                } catch (Exception e) {
                    log.error("出错了，关闭channel", e);
                    try {
                        channel.close();
                    } catch (Exception ignored) {
                    }
                    return false;
                }
            } else {
                log.error("这是不可以的");
                return false;
            }
            return true;
        }
    }
}
