package rpc;

import lombok.SneakyThrows;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import rpc.model.requestresponse.Request;
import rpc.model.requestresponse.Response;
import util.FSTUtil;
import util.ThreadUtil;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

/**
 * @author naison
 * @since 3/14/2020 15:46
 */
public class Client {
    private static final Logger log = LogManager.getLogger(Client.class);

    private static final ConcurrentHashMap<InetSocketAddress, SocketChannel> connections = new ConcurrentHashMap<>();// 主节点于各个简单的链接
    private static Selector selector;// 这个selector处理的是请求的回包

    private static ConcurrentHashMap<Integer, Response> responseMap = new ConcurrentHashMap<>();
    private static LinkedBlockingDeque<RowRequest> requestTask = new LinkedBlockingDeque<>();
    private static Thread sendRequest = new Thread(Client::writeRequest);

    static {
        try {
            selector = Selector.open();
            ThreadUtil.getThreadPool().execute(Client::readResponse);
            sendRequest.start();
        } catch (IOException e) {
            log.error("at the beginning error occurred, shutting down...", e);
            Runtime.getRuntime().exit(-1);
        }
    }

    private static SocketChannel getConnection(InetSocketAddress remote) {
        if (remote == null) return null;

        if (!connections.containsKey(remote) || !connections.get(remote).isOpen() || !connections.get(remote).isConnected()) {
            synchronized (remote.toString().intern()) {
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

    @SneakyThrows
    public static Response doRequest(InetSocketAddress remote, final Request request) {
        if (remote == null) return null;

        requestTask.put(new RowRequest(remote, request));
        LockSupport.unpark(sendRequest);

        int m = 0;
        int t = 500;
        while (m++ < t) {
            if (!responseMap.containsKey(request.requestId)) {
                ThreadUtil.sleep(1);
            } else {
                break;
            }
        }
        return responseMap.remove(request.requestId);
    }

    private static void writeRequest() {
        while (true) {
            while (!requestTask.isEmpty()) {
                RowRequest poll = requestTask.poll();
                if (poll != null) {
                    int retry = 0;
                    while (retry++ < 3) {
                        SocketChannel channel = getConnection(poll.address);
                        if (channel != null) {
                            try {
                                // todo 尝试使用DirectByteBuffer实现零拷贝
                                int write = channel.write(ByteBuffer.wrap(FSTUtil.getConf().asByteArray(poll.request)));
                                if (write <= 0) throw new IOException("魔鬼！！！");
                                break;
                            } catch (IOException e) {
                                log.error(e); // 这里可能出现的情况是对方关闭了channel，该怎么办呢？
                            }
                        }
                    }
                }
            }
            LockSupport.park();
        }
    }


    private static void readResponse() {
        Consumer<SelectionKey> action = key -> {
            if (key.isReadable()) {
                SocketChannel channel = (SocketChannel) key.channel();
                ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
                int read = 0;
                try {
                    read = channel.read(byteBuffer);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                if (read > 0) {
                    Response response = (Response) FSTUtil.getConf().asObject(byteBuffer.array());
                    responseMap.put(response.requestId, response);
                }
            }
        };

        while (true) {
            try {
                int i = selector.selectNow(action);
                if (i == 0) {
                    ThreadUtil.sleep(1);// 忙中偷闲
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    private static class RowRequest {
        public InetSocketAddress address;
        public Request request;

        private RowRequest(InetSocketAddress address, Request request) {
            this.address = address;
            this.request = request;
        }
    }

}
