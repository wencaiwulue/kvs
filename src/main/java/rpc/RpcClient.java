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
import java.net.SocketException;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

/**
 * @author naison
 * @since 3/14/2020 15:46
 */
public class RpcClient {
    private static final Logger LOGGER = LogManager.getLogger(RpcClient.class);

    // todo 其实这里可以是address -> list<channel>, 需要支持多个连接
    private static final ConcurrentHashMap<NodeAddress, SocketChannel> CONNECTIONS = new ConcurrentHashMap<>();// 主节点于各个简单的链接
    private static Selector selector;// 这个selector处理的是请求的回包

    private static final ConcurrentHashMap<Integer, Response> RESPONSE_MAP = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<Integer, CountDownLatch> RESPONSE_MAP_LOCK = new ConcurrentHashMap<>();
    private static final LinkedBlockingDeque<SocketRequest> REQUEST_TASK = new LinkedBlockingDeque<>();
    private static final Thread WRITE_REQUEST_TASK = new Thread(RpcClient::writeRequest, "rpc-write-out");

    static {
        try {
            selector = Selector.open();
            ThreadUtil.getThreadPool().submit(RpcClient::readResponse);
            WRITE_REQUEST_TASK.start();
        } catch (IOException e) {
            LOGGER.error("at the beginning error occurred, shutting down...", e);
            Runtime.getRuntime().exit(-1);
        }
    }

    private static SocketChannel getConnection(NodeAddress remote) {
        if (remote == null) return null;

        if (!CONNECTIONS.containsKey(remote) || !CONNECTIONS.get(remote).isOpen() || !CONNECTIONS.get(remote).isConnected()) {
            synchronized (remote.toString().intern()) {
                if (!CONNECTIONS.containsKey(remote) || !CONNECTIONS.get(remote).isOpen() || !CONNECTIONS.get(remote).isConnected()) {
                    try {
                        SocketChannel channel = SocketChannel.open(remote.getSocketAddress());
                        channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
                        channel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
                        channel.configureBlocking(false);
                        channel.register(selector, SelectionKey.OP_READ);
                        CONNECTIONS.put(remote, channel);
                    } catch (ConnectException e) {
                        LOGGER.error("remote: {}, 连接失败, message: {}。", remote.getSocketAddress().getPort(), e.getMessage());
                        remote.alive = false;
                    } catch (IOException e) {
                        LOGGER.error(e);
                        remote.alive = false;
                    }
                }
            }
        }
        return CONNECTIONS.get(remote);
    }

    public static Response doRequest(NodeAddress remote, final Request request) {
        if (remote == null || !remote.alive) return null;

        CountDownLatch latch = new CountDownLatch(1);
        REQUEST_TASK.addLast(new SocketRequest(remote, request));
        RESPONSE_MAP_LOCK.put(request.requestId, latch);
        LockSupport.unpark(WRITE_REQUEST_TASK);

        try {
            boolean a = latch.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOGGER.error("waiting for response timeout !!!", e);
            return null;
        }
        return RESPONSE_MAP.remove(request.requestId);
    }

    private static void writeRequest() {
        while (true) {
            while (!REQUEST_TASK.isEmpty()) {
                SocketRequest socketRequest = REQUEST_TASK.poll();
                if (socketRequest != null) {
                    boolean success = false;
                    int retry = 0;
                    while (retry++ < 3) {
                        SocketChannel channel = getConnection(socketRequest.address);
                        if (channel != null && socketRequest.address.alive) {
                            try {
                                int write = channel.write(FSTUtil.asArrayWithLength(socketRequest.request));
                                if (write <= 0) throw new IOException("魔鬼！！！");
                                success = true;
                                break;
                            } catch (IOException e) {
                                LOGGER.error("write request error ", e); // 这里可能出现的情况是对方关闭了channel，该怎么办呢？
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
                ByteBuffer contentLen = ByteBuffer.allocateDirect(4);
                try {
                    int read = channel.read(contentLen);
                    if (read > 0) {
                        try {
                            contentLen.flip();
                            int len = contentLen.getInt();
                            if (len > 0) {
                                ByteBuffer result = ByteBuffer.allocate(len);
                                if (len == channel.read(result)) {
                                    Response response = (Response) FSTUtil.getConf().asObject(result.array());
                                    if (response != null) {
                                        RESPONSE_MAP.put(response.requestId, response);
                                        RESPONSE_MAP_LOCK.get(response.requestId).countDown();
                                        RESPONSE_MAP_LOCK.remove(response.requestId);
                                    }
                                }
                            }
                        } catch (OutOfMemoryError e) {
                            LOGGER.error(e);
                            System.out.println("len: " + contentLen.getInt());
                        }
                    } else if (read < 0) {
                        if (key.isValid()) {
                            key.cancel();
                        }
                    }

                } catch (SocketException e) {
                    key.channel();
                    try {
                        channel.close();
                    } catch (IOException ioException) {
                        ioException.printStackTrace();
                    }
                    LOGGER.error(e.getMessage());
                } catch (ClosedChannelException e) {
                    key.channel();
                    try {
                        channel.close();
                    } catch (IOException ioException) {
                        ioException.printStackTrace();
                    }
                    LOGGER.error("channel关闭了");
                } catch (IOException e) {
                    e.printStackTrace();
                    if (e.getMessage().contains("An existing connection was forcibly closed by the remote host")) {
                        key.cancel();
                        try {
                            channel.close();
                        } catch (Exception ignored) {
                        }
                    }
                }

            }
        };

        while (true) {
            try {
                selector.selectNow(action);
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
