package rpc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import rpc.model.requestresponse.Request;
import rpc.model.requestresponse.Response;
import util.FSTUtil;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * @author naison
 * @since 3/14/2020 15:46
 */
public class Client {
    private static final Logger log = LogManager.getLogger(Client.class);

    private static final ConcurrentHashMap<InetSocketAddress, SocketChannel> connections = new ConcurrentHashMap<>();// 主节点于各个简单的链接
    private static Selector selector;// 这个selector处理的是请求的回包
    // todo optimize for huge request, try to use future
    private static LinkedBlockingDeque deque = new LinkedBlockingDeque(50); // each time only allow 50 request wait to process

    static {
        try {
            selector = Selector.open();
        } catch (IOException e) {
            log.error("at the beginning error occurred, shutting down...", e);
            Runtime.getRuntime().exit(-1);
        }
    }

    private static SocketChannel getConnection(InetSocketAddress remote) {
        if (remote == null) return null;

        if (!connections.containsKey(remote) || !connections.get(remote).isOpen() || !connections.get(remote).isConnected()) {
            synchronized (Client.class) {
                if (!connections.containsKey(remote) || !connections.get(remote).isOpen() || !connections.get(remote).isConnected()) {
                    try {
                        SocketChannel channel = SocketChannel.open(remote);
                        channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
                        channel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
                        channel.configureBlocking(false);
                        channel.register(selector, SelectionKey.OP_READ);
                        connections.put(remote, channel);
                    } catch (ConnectException e) {
                        log.error("出错啦, 可能是有的主机死掉了，这个直接吞了，{}", e.getMessage());
                    } catch (IOException e) {
                        log.error(e);
                    }
                }
            }
        }
        return connections.get(remote);
    }

    public static Response doRequest(InetSocketAddress remote, final Request request) {
        if (remote == null) return null;

        SocketChannel channel = getConnection(remote);
        if (channel != null) {
            synchronized (remote.toString().intern()) {// 相同的地址会被锁住
                int retry = 1;
                int t = 0;
                while (t++ < retry) {
                    try {
                        // todo 尝试使用DirectByteBuffer实现零拷贝
                        int write = channel.write(ByteBuffer.wrap(FSTUtil.getConf().asByteArray(request)));
                        if (write <= 0) throw new IOException("魔鬼！！！");

                        return getRes(channel);
                    } catch (IOException e) {
                        log.error(e); // 这里可能出现的情况是对方关闭了channel，该怎么办呢？
                        if (retry++ > 10) break;
                    }
                }
            }
        }
        return null;
    }

    private static Response getRes(SocketChannel channel) throws IOException {
        int retry = 3;
        int t = 0;
        while (t++ < retry) {
            int select = selector.select(10);// 其实这里可以再给时间长一点儿
            if (select > 0) {
                Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
                while (iterator.hasNext()) {
                    SelectionKey key = iterator.next();
                    if (key.channel().equals(channel)) {
                        iterator.remove();
                        if (key.isReadable()) {
                            ByteBuffer buffer = ByteBuffer.allocate(1024);// todo optimize
                            int read = ((SocketChannel) key.channel()).read(buffer);
                            if (read > 0) {
                                return (Response) FSTUtil.getConf().asObject(buffer.array());
                            }
                        } else if (key.isAcceptable() || key.isConnectable() || key.isWritable()) {
                            log.error("这也是魔鬼");
                        }
                    }
                }
            }
        }
        return null;
    }
}
