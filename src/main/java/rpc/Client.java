package rpc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import raft.NodeAddress;
import rpc.model.requestresponse.Request;
import rpc.model.requestresponse.Response;
import util.FSTUtil;
import util.ThreadUtil;

import java.io.IOException;
import java.net.ConnectException;
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

    private static final ConcurrentHashMap<NodeAddress, SocketChannel> connections = new ConcurrentHashMap<>();// 主节点于各个简单的链接
    private static Selector selector;// 这个selector处理的是请求的回包

    private static final ConcurrentHashMap<Integer, Response> responseMap = new ConcurrentHashMap<>();
    private static final LinkedBlockingDeque<SocketRequest> requestTask = new LinkedBlockingDeque<>();
    private static final Thread sendRequest = new Thread(Client::writeRequest);

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

    private static SocketChannel getConnection(NodeAddress remote) {
        if (remote == null) return null;

        if (!connections.containsKey(remote) || !connections.get(remote).isOpen() || !connections.get(remote).isConnected()) {
            synchronized (remote.toString().intern()) {
                if (!connections.containsKey(remote) || !connections.get(remote).isOpen() || !connections.get(remote).isConnected()) {
                    try {
                        SocketChannel channel = SocketChannel.open(remote.socketAddress);
                        channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
                        channel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
                        channel.configureBlocking(false);
                        channel.register(selector, SelectionKey.OP_READ);
                        connections.put(remote, channel);
                    } catch (ConnectException e) {
                        log.error("remote:{}, 连接失败, message:{}。", remote.socketAddress.getPort(), e.getMessage());
                        remote.alive = false;
                    } catch (IOException e) {
                        log.error(e);
                    }
                }
            }
        }
        return connections.get(remote);
    }

    public static Response doRequest(NodeAddress remote, final Request request) {
        if (remote == null || !remote.alive) return null;

        requestTask.addLast(new SocketRequest(remote, request));
        LockSupport.unpark(sendRequest);

        int m = 3; // 2^3 = 8
        int t = 8; // 2^8 = 256
        while (m++ < t) {
            if (!responseMap.containsKey(request.requestId)) {
                ThreadUtil.sleep(1 << t);
            } else {
                break;
            }
        }
        return responseMap.remove(request.requestId);
    }

    private static void writeRequest() {
        while (true) {
            while (!requestTask.isEmpty()) {
                SocketRequest socketRequest = requestTask.poll();
                if (socketRequest != null) {
                    boolean success = false;
                    int retry = 0;
                    while (retry++ < 3) {
                        SocketChannel channel = getConnection(socketRequest.address);
                        if (channel != null && socketRequest.address.alive) {
                            try {
                                // todo 尝试使用DirectByteBuffer实现零拷贝
                                int write = channel.write(ByteBuffer.wrap(FSTUtil.getConf().asByteArray(socketRequest.request)));
                                if (write <= 0) throw new IOException("魔鬼！！！");
                                success = true;
                                break;
                            } catch (IOException e) {
                                log.error(e); // 这里可能出现的情况是对方关闭了channel，该怎么办呢？
                            }
                        }
                    }
                    if (!success) {
                        socketRequest.address.alive = false;
                    }
                }
            }
            LockSupport.park();
        }
    }

    @SuppressWarnings("InfiniteLoopStatement")
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
                    if (e.getMessage().contains("An existing connection was forcibly closed by the remote host")) {
                        key.cancel();
                    }
                }
                if (read > 0) {
                    Response response = (Response) FSTUtil.getConf().asObject(byteBuffer.array());
                    responseMap.put(response.requestId, response);
                } else {
                    if (key.isValid()) {
                        key.cancel();
                    }
                    try {
                        channel.close();
                    } catch (Exception ignored) {
                    }

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


    private static class SocketRequest {
        public NodeAddress address;
        public Request request;

        private SocketRequest(NodeAddress address, Request request) {
            this.address = address;
            this.request = request;
        }
    }

}
